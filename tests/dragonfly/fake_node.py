import asyncio
from typing import Literal, Optional


class _FakeNode:
    """Minimal master that answers the replication handshake line by line."""

    def __init__(self):
        self.host = "127.0.0.1"
        self.port = 0
        self.disconnected = asyncio.Event()
        self._server = None
        self._handlers = set()

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

    def _reply(self, command: list) -> Optional[bytes]:
        """Returns the reply to the command, or None to send nothing back."""
        if command[0] == b"PING":
            return b"+PONG\r\n"
        if command[0] == b"REPLCONF":
            return None if command[1] == b"ACK" else b"+OK\r\n"
        return b"-ERR unsupported command\r\n"

    async def _handle(self, reader, writer):
        task = asyncio.current_task()
        self._handlers.add(task)
        try:
            # Dragonfly sends the replication handshake as inline commands.
            while line := await reader.readline():
                command = line.split()
                if not command:
                    continue
                if (reply := self._reply(command)) is not None:
                    writer.write(reply)
                    await writer.drain()
        except ConnectionError:
            pass
        finally:
            writer.close()
            self._handlers.discard(task)
            self.disconnected.set()


class FakeRedisNode(_FakeNode):
    """Serve RDB bytes using Redis's length or EOF framing.

    Diskless replication uses an EOF marker because the size is not known in advance.
    """

    def __init__(self, rdb: bytes, *, rdb_framing: Literal["length", "eof"] = "length"):
        super().__init__()
        self.sync_attempts = 0

        if rdb_framing == "length":
            payload = f"${len(rdb)}\r\n".encode() + rdb
        elif rdb_framing == "eof":
            marker = b"b" * 40
            payload = b"$EOF:" + marker + b"\r\n" + rdb + marker
        else:
            raise ValueError(f"Unknown RDB framing: {rdb_framing}")
        self._sync_response = b"+FULLRESYNC " + b"a" * 40 + b" 0\r\n" + payload

    def _reply(self, command):
        if command[0] == b"PSYNC":
            self.sync_attempts += 1
            return self._sync_response
        return super()._reply(command)


class FakeDflyNode(_FakeNode):
    """Dragonfly master that completes the greeting but never answers DFLY FLOW."""

    def __init__(self, num_flows: int):
        super().__init__()
        self.num_flows = num_flows
        self.flow_requests = 0
        self.all_flows_hung = asyncio.Event()

    def _reply(self, command):
        if command[:3] == [b"REPLCONF", b"capa", b"dragonfly"]:
            # <master_repl_id> <sync_id> <num_flows>
            return (
                b"*3\r\n$40\r\n" + b"a" * 40 + f"\r\n$5\r\nSYNC1\r\n:{self.num_flows}\r\n".encode()
            )
        if command[:2] == [b"DFLY", b"FLOW"]:
            self.flow_requests += 1
            if self.flow_requests >= self.num_flows:
                self.all_flows_hung.set()
            return None
        return super()._reply(command)
