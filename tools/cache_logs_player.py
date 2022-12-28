#!/usr/bin/env python3
import argparse
from datetime import datetime
import aioredis
import asyncio
from aiocsv import AsyncReader
import aiofiles

'''
To install: pip install -r requirements.txt
'''


class Command:
    args = None
    client_id = 0

class TwitterCacheTraceParser:
    """
    https://github.com/twitter/cache-trace
    """
    def parse(self, csv) -> Command:
        operation = csv[5]
        key = csv[1] + "a"
        value_size = int(csv[3])
        synthetic_value = "".zfill(value_size)

        client_id = csv[4]
        ttl = csv[6]

        cmd = Command()
        cmd.client_id = client_id

        if operation == "get":
            cmd.args = ["GET", key]
        elif operation == 'gets':
            cmd.args = ["GET", key]
        elif operation == 'set':
            cmd.args = ["SET", key, synthetic_value]
        elif operation == 'add':
            cmd.args = ["SET", key, synthetic_value]
        elif operation == 'replace':
            cmd.args = ["SET", key, synthetic_value]
        elif operation == 'cas':
            cmd.args = ["SET", key, synthetic_value]
        elif operation == 'append':
            cmd.args = ["SET", key, synthetic_value]
        elif operation == 'prepend':
            cmd.args = ["SET", key, synthetic_value]
        elif operation == 'delete':
            cmd.args = ["DEL", key]
        elif operation == 'incr':
            cmd.args = ["INCR", key]
        elif operation == 'decr':
            cmd.args = ["DECR", key]

        return cmd

class AsyncWorker:
    QUEUE_SIZE = 100000
    BATCH_SIZE = 20

    def __init__(self, redis_client) -> None:
        self.queue = asyncio.Queue(self.QUEUE_SIZE)
        self.redis_client = redis_client
        self.working = False

    async def put(self, cmd: Command) -> None:
        await self.queue.put(cmd)

    async def work(self) -> None:
        self.working = True
        batch = []
        while self.working or len(batch) > 0 or not self.queue.empty() :
            try:
                cmd = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                batch.append(cmd)
                if len(batch) >= self.BATCH_SIZE:
                    await self.execute(batch)
                    batch.clear()
            except asyncio.exceptions.TimeoutError:
                await self.execute(batch)
                batch.clear()

    async def execute(self, batch) -> None:
        async with self.redis_client.pipeline(transaction=False) as pipe:
            for cmd in batch:
                pipe.execute_command(*cmd.args)
            await pipe.execute()

    def start(self) -> asyncio.Task:
        return asyncio.create_task(self.work())

    def stop(self) -> None:
        self.working = False

class AsyncWorkerPool:
    """
    Mangaes worker pool to send commands in parallel
    Maintains synchronous order for commands with the same client id
    """
    def __init__(self, redis_client, num_workers) -> None:
        self.redis_client = redis_client
        self.num_workers = num_workers
        self.workers = []
        self.tasks = []
        self.client_id_to_worker = {}
        self.next_worker_index = -1

    def allocate(self, client_id) -> AsyncWorker:
        if not client_id in self.client_id_to_worker:
            self.next_worker_index = (self.next_worker_index + 1) % self.num_workers

            if len(self.workers) <= self.next_worker_index:
                assert len(self.workers) == self.next_worker_index
                self.workers.append(AsyncWorker(self.redis_client))
                self.tasks.append(self.workers[self.next_worker_index].start())

            self.client_id_to_worker[client_id] = self.workers[self.next_worker_index]

        return self.client_id_to_worker[client_id]

    async def put(self, cmd: Command) -> None:
        worker = self.allocate(cmd.client_id)
        await worker.put(cmd)

    async def stop(self):
        for worker in self.workers:
            worker.stop()
        await asyncio.gather(*self.tasks)


class AsyncPlayer:
    def __init__(self, redis_uri, num_workers) -> None:
        self.redis_uri = redis_uri
        self.redis_client = aioredis.from_url(f"redis://{self.redis_uri}", encoding="utf-8", decode_responses=True)
        self.worker_pool = AsyncWorkerPool(self.redis_client, 100)

    async def read_and_dispatch(self, csv_file, parser):
        print(f"dispatching from {csv_file}")
        async with aiofiles.open(csv_file, mode="r", encoding="utf-8", newline="") as afp:
            async for row in AsyncReader(afp):
                await self.worker_pool.put(parser.parse(row))

    async def reportStats(self):
        while True:
            info = await self.redis_client.execute_command("info", "stats")
            print(f"{datetime.now()}: {info}")
            await asyncio.sleep(10)


    async def play(self, csv_file, parser) -> None:
        print(f"pinging {self.redis_uri} successful?")
        print(await self.redis_client.ping())

        read_dispatch_task = asyncio.create_task(self.read_and_dispatch(csv_file, parser))
        stats_task = asyncio.create_task(self.reportStats())

        await read_dispatch_task
        print(f"finished reading {csv_file}")

        await self.worker_pool.stop()

        stats_task.cancel()
        print("all done")

def main():
    parser = argparse.ArgumentParser(description='Cache Logs Player')
    parser.add_argument('-u', '--uri', type=str, default='localhost:6379', help='Redis server URI')
    parser.add_argument('-f', '--csv_file', type=str, default='/home/ari/Downloads/cluster017.csv', help='Redis server URI')
    parser.add_argument('--num_workers', type=int, default=100, help='Maximum number of workers sending commands in parllel')

    args = parser.parse_args()

    player = AsyncPlayer(redis_uri=args.uri, num_workers=args.num_workers)
    asyncio.run(player.play(args.csv_file, TwitterCacheTraceParser()))

if __name__ == "__main__":
    main()
