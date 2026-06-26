import asyncio
import random


class Proxy:
    def __init__(self, host, port, remote_host, remote_port):
        self.host = host
        self.port = port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.stop_connections = []
        self.server = None
        self._handler_tasks = set()
        self._serve_task = None

    async def handle(self, reader, writer):
        task = asyncio.current_task()
        self._handler_tasks.add(task)
        try:
            await self._handle_impl(reader, writer)
        finally:
            self._handler_tasks.discard(task)

    async def _handle_impl(self, reader, writer):
        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                self.remote_host, self.remote_port
            )
        except (OSError, asyncio.CancelledError):
            writer.close()
            return

        async def forward(reader, writer):
            while True:
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

        self.stop_connections.append(cleanup)

        try:
            await asyncio.gather(task1, task2)
        except (asyncio.CancelledError, ConnectionResetError):
            pass
        finally:
            cleanup()
            if cleanup in self.stop_connections:
                self.stop_connections.remove(cleanup)

    async def start(self):
        self.server = await asyncio.start_server(self.handle, self.host, self.port)

        if self.port == 0:
            _, port = self.server.sockets[0].getsockname()[:2]
            self.port = port

    async def start_serving(self):
        await self.start()
        self._serve_task = asyncio.create_task(self.serve(self.server))

    async def serve(self, server=None):
        server = server or self.server
        if server is None:
            return
        async with server:
            await server.serve_forever()

    async def __aenter__(self):
        await self.start_serving()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def drop_connection(self):
        """
        Randomly drop one connection
        """
        if self.stop_connections:
            cb = random.choice(self.stop_connections)
            self.stop_connections.remove(cb)
            cb()

    async def close(self, task=None):
        if task is None:
            task = self._serve_task
        if task is self._serve_task:
            self._serve_task = None

        if self.server is not None:
            self.server.close()
            self.server = None

        for cb in self.stop_connections:
            cb()
        self.stop_connections = []

        # Yield so that accepted-but-not-yet-started handler tasks begin
        # executing and register themselves in _handler_tasks.
        await asyncio.sleep(0)

        # Cancel all handler tasks, including ones that haven't registered
        # their cleanup callbacks yet (race between accept and close).
        # Loop until no tasks remain so late-starting handlers are caught.
        while self._handler_tasks:
            tasks = list(self._handler_tasks)
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        if task is not None:
            try:
                await task
            except asyncio.exceptions.CancelledError:
                pass
