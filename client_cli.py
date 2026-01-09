import asyncio, json, sys, uuid
from protocol import MsgType, send_message, read_message

async def main():
    if len(sys.argv) < 3:
        print("uso: python client_cli.py <host> <port>")
        return

    host = sys.argv[1]
    port = int(sys.argv[2])

    print("Digite SQL e pressione Enter. 'exit' para sair.")
    while True:
        sql = input("SQL> ").strip()
        if not sql:
            continue
        if sql.lower() == "exit":
            break

        req_id = str(uuid.uuid4())
        reader, writer = await asyncio.open_connection(host, port)
        await send_message(writer, MsgType.CLIENT_QUERY, {"sql": sql, "request_id": req_id, "client": "cli"})
        _, payload = await read_message(reader)
        writer.close(); await writer.wait_closed()

        print("\n--- RESULT ---")
        print("ok:", payload.get("ok"))
        print("executed_on_node:", payload.get("executed_on_node"))
        if payload.get("ok"):
            if "columns" in payload:
                print("columns:", payload["columns"])
                print("rows:", payload["rows"])
            else:
                print(payload.get("message"))
        else:
            print("error:", payload.get("error"))
            if "prepare_results" in payload:
                print("prepare_results:", payload["prepare_results"])
        print("-------------\n")

if __name__ == "__main__":
    asyncio.run(main())