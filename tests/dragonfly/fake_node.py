import asyncio
from typing import Literal


class FakeRedisNode:
    """Serve RDB bytes using Redis's length or EOF framing.

    Diskless replication uses an EOF marker because the size is not known in advance.
    """

    def __init__(self, rdb: bytes, *, rdb_framing: Literal["length", "eof"] = "length"):
        self.host = "127.0.0.1"
        self.port = 0
        self.sync_attempts = 0
        self.disconnected = asyncio.Event()
        self._server = None
        self._handlers = set()

        if rdb_framing == "length":
            payload = f"${len(rdb)}\r\n".encode() + rdb
        elif rdb_framing == "eof":
            marker = b"b" * 40
            payload = b"$EOF:" + marker + b"\r\n" + rdb + marker
        else:
            raise ValueError(f"Unknown RDB framing: {rdb_framing}")
        self._sync_response = b"+FULLRESYNC " + b"a" * 40 + b" 0\r\n" + payload

    async def __aenter__(self):
        self._server = await asyncio.start_server(self._handle, self.host, 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._server.close()
        # Let accepted connections register their handlers before cancelling them.
        await asyncio.sleep(0)
        while self._handlers:
            handlers = list(self._handlers)
            for handler in handlers:
                handler.cancel()
            await asyncio.gather(*handlers, return_exceptions=True)
        await self._server.wait_closed()

    async def _handle(self, reader, writer):
        task = asyncio.current_task()
        self._handlers.add(task)
        try:
            # Dragonfly sends the replication handshake as inline commands.
            while line := await reader.readline():
                command = line.split()
                if not command:
                    continue
                if command[0] == b"PING":
                    writer.write(b"+PONG\r\n")
                elif command[0] == b"REPLCONF":
                    if command[1] == b"ACK":
                        continue
                    writer.write(b"+OK\r\n")
                elif command[0] == b"PSYNC":
                    self.sync_attempts += 1
                    writer.write(self._sync_response)
                else:
                    writer.write(b"-ERR unsupported command\r\n")
                await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()
            self._handlers.discard(task)
            self.disconnected.set()
