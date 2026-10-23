import asyncio
import logging
import random
import socket


class Proxy:
    def __init__(self, host, port, remote_host, remote_port):
        self.host = host
        self.port = port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.stop_connections = []
        self._listener = None
        self._accept_retry = None
        # Accepted sockets that no handler has taken over yet.
        self._accepted = set()
        self._handler_tasks = set()
        self._forwarding = asyncio.Event()
        self._forwarding.set()
        self._conn_gates = []

    async def handle(self, sock):
        self._accepted.discard(sock)
        try:
            reader, writer = await asyncio.open_connection(sock=sock)
        except (OSError, asyncio.CancelledError):
            sock.close()
            return
        await self._handle_impl(reader, writer)

    async def _handle_impl(self, reader, writer):
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                self.remote_host, self.remote_port
            )
        except (OSError, asyncio.CancelledError):
            writer.close()
            return

        gate = asyncio.Event()
        gate.set()
        self._conn_gates.append(gate)

        async def forward(reader, writer):
            while True:
                await self._forwarding.wait()
                await gate.wait()
                data = await reader.read(1024)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
            writer.close()

        task1 = asyncio.ensure_future(forward(reader, remote_writer))
        task2 = asyncio.ensure_future(forward(remote_reader, writer))

        def cleanup():
            task1.cancel()
            task2.cancel()
            writer.close()
            remote_writer.close()
            if gate in self._conn_gates:
                self._conn_gates.remove(gate)

        self.stop_connections.append(cleanup)

        try:
            await asyncio.gather(task1, task2)
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            cleanup()
            if cleanup in self.stop_connections:
                self.stop_connections.remove(cleanup)

    def _accept(self):
        # Accept and register in the same loop callback: asyncio.Server hands an accepted socket
        # to a task that runs later, and if the server is closed before that task runs the socket
        # stays open, unreachable from close(), until the cyclic GC frees it.
        while True:
            try:
                sock, _ = self._listener.accept()
            except (BlockingIOError, InterruptedError, ConnectionAbortedError):
                return
            except OSError as e:
                # E.g. out of fds: the listener stays readable, so back off instead of spinning.
                logging.warning("Proxy accept failed, retrying in 1s: %s", e)
                loop = asyncio.get_running_loop()
                loop.remove_reader(self._listener.fileno())
                self._accept_retry = loop.call_later(1, self._resume_accept)
                return
            sock.setblocking(False)
            self._accepted.add(sock)
            task = asyncio.create_task(self.handle(sock))
            self._handler_tasks.add(task)
            task.add_done_callback(self._handler_tasks.discard)

    def _resume_accept(self):
        self._accept_retry = None
        asyncio.get_running_loop().add_reader(self._listener.fileno(), self._accept)

    async def start_serving(self):
        family = socket.AF_INET6 if ":" in self.host else socket.AF_INET
        self._listener = socket.create_server((self.host, self.port), family=family, backlog=100)
        self._listener.setblocking(False)
        if self.port == 0:
            self.port = self._listener.getsockname()[1]
        asyncio.get_running_loop().add_reader(self._listener.fileno(), self._accept)

    async def __aenter__(self):
        await self.start_serving()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def pause(self):
        """
        Stop forwarding bytes without closing anything. Sockets stay ESTABLISHED on both
        sides while data stops moving, which is what either a lost network path or a peer
        that stopped reading looks like to the sender (as opposed to drop_connection, which
        is an immediate reset).
        """
        self._forwarding.clear()

    def pause_one(self, index=0):
        """
        Stall a single connection and leave the rest forwarding. With one replication
        flow per shard, this freezes exactly one flow.
        """
        gates = list(self._conn_gates)
        if not gates:
            return False
        gates[index % len(gates)].clear()
        return True

    def connection_count(self):
        """Number of currently proxied connections."""
        return len(self._conn_gates)

    def resume(self):
        """Undo pause()/pause_one(): resume forwarding on all connections."""
        self._forwarding.set()
        for gate in list(self._conn_gates):
            gate.set()

    def drop_connection(self):
        """
        Randomly drop one connection
        """
        if self.stop_connections:
            cb = random.choice(self.stop_connections)
            self.stop_connections.remove(cb)
            cb()

    async def close(self):
        # A Proxy is reused across close()/start_serving() cycles (see test_partial_sync), so
        # leave it forwarding: a paused proxy would silently stall every future connection.
        self.resume()

        if self._accept_retry is not None:
            self._accept_retry.cancel()
            self._accept_retry = None

        if self._listener is not None:
            asyncio.get_running_loop().remove_reader(self._listener.fileno())
            self._listener.close()
            self._listener = None

        for cb in self.stop_connections:
            cb()
        self.stop_connections = []

        # No accepts happen past this point and every accepted socket already has a handler
        # task, including the ones that have not started running yet.
        tasks = list(self._handler_tasks)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        for sock in self._accepted:
            sock.close()
        self._accepted.clear()
