import asyncio, json, struct, zlib
from enum import IntEnum

MAGIC = b"DDB1"
VERSION = 1

class MsgType(IntEnum):
    JOIN = 1
    JOIN_ACK = 2
    HEARTBEAT = 3

    ELECTION = 10
    ELECTION_OK = 11
    COORDINATOR_ANNOUNCE = 12

    CLIENT_QUERY = 20
    QUERY_RESULT = 21

    PREPARE = 30
    PREPARED = 31
    ABORT = 32
    COMMIT = 33
    ROLLBACK = 34

HEADER_FMT = "!4sBBII"  # magic, ver, type, length, crc32
HEADER_SIZE = struct.calcsize(HEADER_FMT)

def pack_message(msg_type: MsgType, payload: dict) -> bytes:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    crc = zlib.crc32(data) & 0xffffffff
    header = struct.pack(HEADER_FMT, MAGIC, VERSION, int(msg_type), len(data), crc)
    return header + data

async def read_exact(reader: asyncio.StreamReader, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = await reader.read(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed")
        buf += chunk
    return buf

async def read_message(reader: asyncio.StreamReader) -> tuple[MsgType, dict]:
    header = await read_exact(reader, HEADER_SIZE)
    magic, ver, mtype, length, crc = struct.unpack(HEADER_FMT, header)

    if magic != MAGIC or ver != VERSION:
        raise ValueError("Invalid magic/version")

    payload_bytes = await read_exact(reader, length)
    calc = zlib.crc32(payload_bytes) & 0xffffffff
    if calc != crc:
        raise ValueError("Checksum mismatch")

    payload = json.loads(payload_bytes.decode("utf-8"))
    return MsgType(mtype), payload

async def send_message(writer: asyncio.StreamWriter, msg_type: MsgType, payload: dict):
    writer.write(pack_message(msg_type, payload))
    await writer.drain()
