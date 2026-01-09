import asyncio, json, time, uuid
import mysql.connector
from mysql.connector import Error
from protocol import MsgType, read_message, send_message

HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0

WRITE_PREFIXES = ("insert", "update", "delete", "create", "alter", "drop", "truncate", "replace")

def is_write(sql: str) -> bool:
    s = sql.strip().lower()
    return s.startswith(WRITE_PREFIXES)

class DDBNode:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.node_id = cfg["node_id"]
        self.listen_host = cfg["listen_host"]
        self.listen_port = cfg["listen_port"]
        self.cluster = {n["node_id"]: (n["host"], n["port"]) for n in cfg["cluster_nodes"]}

        self.coordinator_id = max(self.cluster.keys())  # bully: maior id começa como preferido
        self.last_coord_seen = time.time()

        self.inflight = 0
        self.active_nodes = {nid: {"last": 0, "inflight": 0} for nid in self.cluster.keys()}
        self.txn_prepared = {}  # txn_id -> mysql_connection

        self.server = None

    def mysql_connect(self):
        return mysql.connector.connect(**self.cfg["mysql"], autocommit=False)

    async def start(self):
        self.server = await asyncio.start_server(self.handle_conn, self.listen_host, self.listen_port)
        print(f"[node {self.node_id}] listening on {self.listen_host}:{self.listen_port}")

        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.monitor_coordinator_loop())

        async with self.server:
            await self.server.serve_forever()

    async def handle_conn(self, reader, writer):
        peer = writer.get_extra_info("peername")
        try:
            while True:
                mtype, payload = await read_message(reader)
                await self.dispatch(mtype, payload, writer)
        except Exception as e:
            # conexão caiu ou erro de protocolo
            # print(f"[node {self.node_id}] conn {peer} closed: {e}")
            pass
        finally:
            writer.close()
            await writer.wait_closed()

    async def dispatch(self, mtype: MsgType, payload: dict, writer):
        if mtype == MsgType.JOIN:
            # simples: aceita e responde coord atual
            await send_message(writer, MsgType.JOIN_ACK, {
                "ok": True,
                "node_id": self.node_id,
                "coordinator_id": self.coordinator_id
            })

        elif mtype == MsgType.HEARTBEAT:
            nid = payload["node_id"]
            self.active_nodes[nid] = {
                "last": time.time(),
                "inflight": payload.get("inflight", 0)
            }
            # se veio do coordenador, atualiza último visto
            if nid == self.coordinator_id:
                self.last_coord_seen = time.time()

        elif mtype == MsgType.ELECTION:
            # bully: se eu tenho id maior, respondo OK e inicio minha eleição
            candidate = payload["candidate_id"]
            if self.node_id > candidate:
                await send_message(writer, MsgType.ELECTION_OK, {"ok": True, "from": self.node_id})
                asyncio.create_task(self.start_election())

        elif mtype == MsgType.COORDINATOR_ANNOUNCE:
            newc = payload["coordinator_id"]
            self.coordinator_id = newc
            self.last_coord_seen = time.time()
            print(f"[node {self.node_id}] new coordinator = {newc}")

        elif mtype == MsgType.CLIENT_QUERY:
            # cliente pode conectar em qualquer nó; se não sou coordenador, encaminho
            if self.node_id != self.coordinator_id:
                result = await self.forward_to_coordinator(mtype, payload)
                await send_message(writer, MsgType.QUERY_RESULT, result)
            else:
                result = await self.handle_client_query_as_coordinator(payload)
                await send_message(writer, MsgType.QUERY_RESULT, result)

        elif mtype == MsgType.PREPARE:
            res = await self.handle_prepare(payload)
            await send_message(writer, MsgType.PREPARED if res["ok"] else MsgType.ABORT, res)

        elif mtype == MsgType.COMMIT:
            await self.handle_commit(payload)
        elif mtype == MsgType.ROLLBACK:
            await self.handle_rollback(payload)

    async def forward_to_coordinator(self, mtype, payload):
        host, port = self.cluster[self.coordinator_id]
        reader, writer = await asyncio.open_connection(host, port)
        await send_message(writer, mtype, payload)
        rtype, rpayload = await read_message(reader)
        writer.close(); await writer.wait_closed()
        return rpayload

    async def handle_client_query_as_coordinator(self, payload: dict) -> dict:
        sql = payload["sql"]
        req_id = payload.get("request_id", str(uuid.uuid4()))
        client_tag = payload.get("client", "client")

        # logging exigido: query requisitada + conteúdo transmitido
        print(f"[coord {self.node_id}] CLIENT_QUERY {req_id} from={client_tag} sql={sql!r}")

        if not is_write(sql):
            # READ: balancear
            target = self.pick_read_node()
            out = await self.exec_select_on_node(target, sql, req_id)
            out["executed_on_node"] = target
            return out

        # WRITE: 2PC em todos os nós ativos (incluindo eu)
        txn_id = str(uuid.uuid4())
        participants = self.current_participants()

        # Phase 1: PREPARE
        prep_results = await self.broadcast_prepare(participants, txn_id, sql, req_id)

        if all(r.get("ok") for r in prep_results.values()):
            # Phase 2: COMMIT
            await self.broadcast_commit(participants, txn_id)
            return {
                "ok": True,
                "request_id": req_id,
                "executed_on_node": self.node_id,
                "message": f"COMMITTED on {len(participants)} nodes",
                "rows": prep_results[self.node_id].get("rows", None)
            }
        else:
            await self.broadcast_rollback(participants, txn_id)
            return {
                "ok": False,
                "request_id": req_id,
                "executed_on_node": self.node_id,
                "error": "ABORTED (prepare failed)",
                "prepare_results": prep_results
            }

    def current_participants(self):
        now = time.time()
        alive = []
        for nid in self.cluster.keys():
            if nid == self.node_id:
                alive.append(nid)
            else:
                last = self.active_nodes.get(nid, {}).get("last", 0)
                if now - last <= HEARTBEAT_TIMEOUT:
                    alive.append(nid)
        return sorted(set(alive))

    def pick_read_node(self) -> int:
        # escolhe nó ativo com menor inflight (inclusive eu)
        nodes = self.current_participants()
        best = None
        best_load = 10**9
        for nid in nodes:
            load = self.inflight if nid == self.node_id else self.active_nodes.get(nid, {}).get("inflight", 0)
            if load < best_load:
                best_load = load
                best = nid
        return best or self.node_id

    async def exec_select_on_node(self, nid: int, sql: str, req_id: str) -> dict:
        if nid == self.node_id:
            return await self.exec_local_select(sql, req_id)
        host, port = self.cluster[nid]
        reader, writer = await asyncio.open_connection(host, port)
        await send_message(writer, MsgType.CLIENT_QUERY, {"sql": sql, "request_id": req_id, "client": "coord"})
        _, rpayload = await read_message(reader)
        writer.close(); await writer.wait_closed()
        return rpayload

    async def exec_local_select(self, sql: str, req_id: str) -> dict:
        self.inflight += 1
        try:
            conn = self.mysql_connect()
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            conn.commit()
            return {"ok": True, "request_id": req_id, "columns": cols, "rows": rows, "executed_on_node": self.node_id}
        except Error as e:
            return {"ok": False, "request_id": req_id, "error": str(e), "executed_on_node": self.node_id}
        finally:
            try:
                conn.close()
            except:
                pass
            self.inflight -= 1

    async def handle_prepare(self, payload: dict) -> dict:
        txn_id = payload["txn_id"]
        sql = payload["sql"]
        req_id = payload.get("request_id", "?")

        print(f"[node {self.node_id}] PREPARE txn={txn_id} req={req_id} sql={sql!r}")

        self.inflight += 1
        try:
            conn = self.mysql_connect()
            cur = conn.cursor()
            cur.execute("START TRANSACTION")
            cur.execute(sql)
            # não commit aqui
            self.txn_prepared[txn_id] = conn
            return {"ok": True, "txn_id": txn_id, "node_id": self.node_id}
        except Error as e:
            try:
                conn.rollback()
                conn.close()
            except:
                pass
            return {"ok": False, "txn_id": txn_id, "node_id": self.node_id, "error": str(e)}
        finally:
            self.inflight -= 1

    async def handle_commit(self, payload: dict):
        txn_id = payload["txn_id"]
        conn = self.txn_prepared.pop(txn_id, None)
        if conn:
            print(f"[node {self.node_id}] COMMIT txn={txn_id}")
            conn.commit()
            conn.close()

    async def handle_rollback(self, payload: dict):
        txn_id = payload["txn_id"]
        conn = self.txn_prepared.pop(txn_id, None)
        if conn:
            print(f"[node {self.node_id}] ROLLBACK txn={txn_id}")
            conn.rollback()
            conn.close()

    async def broadcast_prepare(self, participants, txn_id, sql, req_id):
        results = {}
        for nid in participants:
            if nid == self.node_id:
                results[nid] = await self.handle_prepare({"txn_id": txn_id, "sql": sql, "request_id": req_id})
            else:
                host, port = self.cluster[nid]
                reader, writer = await asyncio.open_connection(host, port)
                # logging: conteúdo transmitido
                print(f"[coord {self.node_id}] -> PREPARE to node {nid}: txn={txn_id} sql={sql!r}")
                await send_message(writer, MsgType.PREPARE, {"txn_id": txn_id, "sql": sql, "request_id": req_id})
                rtype, rpayload = await read_message(reader)
                results[nid] = rpayload
                writer.close(); await writer.wait_closed()
        return results

    async def broadcast_commit(self, participants, txn_id):
        for nid in participants:
            if nid == self.node_id:
                await self.handle_commit({"txn_id": txn_id})
            else:
                host, port = self.cluster[nid]
                reader, writer = await asyncio.open_connection(host, port)
                print(f"[coord {self.node_id}] -> COMMIT to node {nid}: txn={txn_id}")
                await send_message(writer, MsgType.COMMIT, {"txn_id": txn_id})
                writer.close(); await writer.wait_closed()

    async def broadcast_rollback(self, participants, txn_id):
        for nid in participants:
            if nid == self.node_id:
                await self.handle_rollback({"txn_id": txn_id})
            else:
                host, port = self.cluster[nid]
                reader, writer = await asyncio.open_connection(host, port)
                print(f"[coord {self.node_id}] -> ROLLBACK to node {nid}: txn={txn_id}")
                await send_message(writer, MsgType.ROLLBACK, {"txn_id": txn_id})
                writer.close(); await writer.wait_closed()

    async def heartbeat_loop(self):
        # envia heartbeat ao coordenador (ou a todos se eu for coord, para manter visão distribuída simples)
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if self.node_id == self.coordinator_id:
                # coord manda broadcast
                for nid, (h, p) in self.cluster.items():
                    if nid == self.node_id:
                        continue
                    try:
                        reader, writer = await asyncio.open_connection(h, p)
                        await send_message(writer, MsgType.HEARTBEAT, {"node_id": self.node_id, "inflight": self.inflight})
                        writer.close(); await writer.wait_closed()
                    except:
                        pass
            else:
                # nó manda unicast para coord
                h, p = self.cluster[self.coordinator_id]
                try:
                    reader, writer = await asyncio.open_connection(h, p)
                    await send_message(writer, MsgType.HEARTBEAT, {"node_id": self.node_id, "inflight": self.inflight})
                    writer.close(); await writer.wait_closed()
                except:
                    pass

    async def monitor_coordinator_loop(self):
        while True:
            await asyncio.sleep(1.0)
            if self.node_id == self.coordinator_id:
                continue
            if time.time() - self.last_coord_seen > HEARTBEAT_TIMEOUT:
                print(f"[node {self.node_id}] coordinator timeout -> election")
                await self.start_election()

    async def start_election(self):
        # bully: envio ELECTION para ids maiores; se ninguém responder OK, eu viro coordenador
        higher = [nid for nid in self.cluster.keys() if nid > self.node_id]
        got_ok = False

        for nid in higher:
            h, p = self.cluster[nid]
            try:
                reader, writer = await asyncio.open_connection(h, p)
                await send_message(writer, MsgType.ELECTION, {"candidate_id": self.node_id})
                rtype, rpayload = await read_message(reader)
                if rtype == MsgType.ELECTION_OK:
                    got_ok = True
                writer.close(); await writer.wait_closed()
            except:
                pass

        if not got_ok:
            # assumo coord e anuncio
            self.coordinator_id = self.node_id
            self.last_coord_seen = time.time()
            print(f"[node {self.node_id}] I AM THE NEW COORDINATOR")
            await self.announce_coordinator()

    async def announce_coordinator(self):
        for nid, (h, p) in self.cluster.items():
            if nid == self.node_id:
                continue
            try:
                reader, writer = await asyncio.open_connection(h, p)
                await send_message(writer, MsgType.COORDINATOR_ANNOUNCE, {"coordinator_id": self.node_id})
                writer.close(); await writer.wait_closed()
            except:
                pass

if __name__ == "__main__":
    import sys
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.json"
    cfg = json.load(open(cfg_path, "r", encoding="utf-8"))
    node = DDBNode(cfg)
    asyncio.run(node.start())