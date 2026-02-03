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
    sync_id = 0 # Commands with the same sync_id will be executed synchrnously

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
        cmd.sync_id = client_id

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
            cmd.args = ["APPEND", key, synthetic_value]
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

    def __init__(self, redis_client) -> None:
        self.queue = asyncio.Queue(self.QUEUE_SIZE)
        self.redis_client = redis_client
        self.working = False

    async def put(self, batch: list) -> None:
        await self.queue.put(batch)

    async def work(self) -> None:
        self.working = True
        while self.working or not self.queue.empty() :
            batch = await self.queue.get()
            await self.execute(batch)

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
    Maintains synchronous order for commands with the same sync_id
    """
    def __init__(self, redis_client, num_workers) -> None:
        self.redis_client = redis_client
        self.num_workers = num_workers
        self.workers = []
        self.tasks = []
        self.sync_id_to_worker = {}
        self.next_worker_index = -1

    def allocate(self, sync_id) -> AsyncWorker:
        if not sync_id in self.sync_id_to_worker:
            self.next_worker_index = (self.next_worker_index + 1) % self.num_workers

            if len(self.workers) <= self.next_worker_index:
                assert len(self.workers) == self.next_worker_index
                self.workers.append(AsyncWorker(self.redis_client))
                self.tasks.append(self.workers[self.next_worker_index].start())

            self.sync_id_to_worker[sync_id] = self.workers[self.next_worker_index]

        return self.sync_id_to_worker[sync_id]

    async def put(self, batch: list, sync_id: int) -> None:
        worker = self.allocate(sync_id)
        await worker.put(batch)

    async def stop(self):
        for worker in self.workers:
            worker.stop()
        await asyncio.gather(*self.tasks)


class AsyncPlayer:
    READ_BATCH_SIZE = 10 * 1000 * 1000

    def __init__(self, redis_uri, num_workers) -> None:
        self.redis_uri = redis_uri
        self.redis_client = aioredis.from_url(f"redis://{self.redis_uri}", encoding="utf-8", decode_responses=True)
        self.worker_pool = AsyncWorkerPool(self.redis_client, 100)

        self.batch_by_sync_id = {}

    async def dispatch_batches(self):
        for sync_id in self.batch_by_sync_id:
            await self.worker_pool.put(self.batch_by_sync_id[sync_id], sync_id)
        self.batch_by_sync_id.clear()

    async def read_and_dispatch(self, csv_file, parser):
        print(f"dispatching from {csv_file}")

        line_count = 0

        async with aiofiles.open(csv_file, mode="r", encoding="utf-8", newline="") as afp:
            async for row in AsyncReader(afp):
                cmd = parser.parse(row)
                if not self.batch_by_sync_id.get(cmd.sync_id):
                    self.batch_by_sync_id[cmd.sync_id] = []
                batch = self.batch_by_sync_id[cmd.sync_id]
                batch.append(cmd)
                line_count = line_count + 1
                if (line_count >= self.READ_BATCH_SIZE):
                    await self.dispatch_batches()
                    line_count = 0
            # handle the remaining lines
            await self.dispatch_batches()

    async def print_stats(self):
        info = await self.redis_client.execute_command("info", "stats")
        print(f"{datetime.now()}: {info}")

    async def report_stats(self):
        while True:
            self.print_stats()

    async def report_stats(self):
        while True:
            await self.print_stats()
            await asyncio.sleep(10)

    async def play(self, csv_file, parser) -> None:
        print(f"pinging {self.redis_uri} successful?")
        print(await self.redis_client.ping())

        read_dispatch_task = asyncio.create_task(self.read_and_dispatch(csv_file, parser))
        stats_task = asyncio.create_task(self.report_stats())

        await read_dispatch_task
        print(f"finished reading {csv_file}")

        await self.worker_pool.stop()
        stats_task.cancel()
        print("all done")
        await self.print_stats()

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
