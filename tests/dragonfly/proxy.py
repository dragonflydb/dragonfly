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

    async def handle(self, reader, writer):
        remote_reader, remote_writer = await asyncio.open_connection(
            self.remote_host, self.remote_port
        )

        async def forward(reader, writer):
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                writer.write(data)
                await writer.drain()

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
        except asyncio.CancelledError:
            pass
        finally:
            cleanup()
            if cleanup in self.stop_connections:
                self.stop_connections.remove(cleanup)

    async def start(self):
        self.server = await asyncio.start_server(self.handle, self.host, self.port)
        async with self.server:
            await self.server.serve_forever()

    def drop_connection(self):
        """
        Randomally drop one connection
        """
        if self.stop_connections:
            cb = random.choice(self.stop_connections)
            self.stop_connections.remove(cb)
            cb()

    def close(self):
        self.server.close()
        for cb in self.stop_connections:
            cb()
