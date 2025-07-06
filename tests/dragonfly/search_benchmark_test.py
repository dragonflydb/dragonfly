import logging
import time
import pytest

from . import dfly_args
from .instance import DflyInstance
from .search_benchmark_utils import (
    generate_document_columns,
    create_search_index,
    generate_document_data,
    run_query_load_test,
    set_random_seed,
    INDEX_KEY,
    DOCUMENT_KEY,
)


@dfly_args({"proactor_threads": 4})
@pytest.mark.opt_only
@pytest.mark.slow
class TestSearchBenchmark:
    random_seed = 42
    num_documents = 3000
    chunk_size = 1000

    @pytest.fixture(scope="class")
    async def prepared_benchmark_data(self, df_server: DflyInstance):
        set_random_seed(self.random_seed)

        logging.info(f"Preparing benchmark data on port {df_server.port}")
        client = df_server.client()

        # Basic connectivity check
        assert await client.ping() == True

        # Schema Generation
        logging.info("Schema Generation - generating columns and creating search index")
        document_columns = generate_document_columns()
        await create_search_index(client, document_columns)

        # Verify the index was created
        index_info = await client.execute_command(f"FT.INFO {INDEX_KEY}")
        assert index_info is not None
        logging.info(f"Search index '{INDEX_KEY}' created with {len(document_columns)} columns")

        # Data Generation
        logging.info(
            f"Data Generation - generating {self.num_documents:,} documents with full column data"
        )
        stage_start = time.time()
        document_ids = await generate_document_data(
            client=client,
            columns=document_columns,
            num_documents=self.num_documents,
            chunk_size=self.chunk_size,  # Chunk size for batch processing
        )

        # Verify data was generated
        assert len(document_ids) == self.num_documents

        # Verify some documents were stored
        sample_document_id = document_ids[0]
        document_key = DOCUMENT_KEY.format(documentId=sample_document_id)
        stored_document = await client.hgetall(document_key)
        assert stored_document is not None
        assert stored_document["DocumentId"] == sample_document_id
        stage_duration = time.time() - stage_start
        logging.info(
            f"Preparation stage completed in {stage_duration:.2f}s: {len(document_ids)} documents generated and stored"
        )

        await client.aclose()

        return {
            "document_columns": document_columns,
            "document_ids": document_ids,
            "num_documents": self.num_documents,
            "setup_duration": stage_duration,
        }

    async def _run_benchmark(
        self,
        df_server: DflyInstance,
        prepared_benchmark_data,
        num_queries: int,
        num_concurrent_clients: int,
        test_name: str,
    ):
        logging.info(f"Starting {test_name} test on port {df_server.port}")
        logging.info(
            f"Parameters: {prepared_benchmark_data['num_documents']} documents, {num_queries} queries, {num_concurrent_clients} concurrent clients"
        )

        client = df_server.client()

        # Basic connectivity check
        assert await client.ping() == True

        # Query Load Testing
        logging.info(
            f"Query Load Testing - running {num_queries:,} queries with {num_concurrent_clients} concurrent clients"
        )
        stage_start = time.time()
        total_completed = await run_query_load_test(
            df_server=df_server,
            columns=prepared_benchmark_data["document_columns"],
            document_ids=prepared_benchmark_data["document_ids"],
            total_queries=num_queries,
            num_concurrent_clients=num_concurrent_clients,
        )

        # Verify queries completed
        assert total_completed == num_queries
        stage_duration = time.time() - stage_start
        logging.info(
            f"Query Load Testing completed in {stage_duration:.2f}s: {total_completed} queries executed successfully"
        )

        # Final summary
        logging.info(
            f"Benchmark Timings Summary -> Data Generation: {prepared_benchmark_data['setup_duration']:.2f}s | Query Load: {stage_duration:.2f}s"
        )

        # Command statistics
        cmd_stats = await client.info("commandstats")
        logging.info("Command Statistics:")
        for key, value in cmd_stats.items():
            if key.startswith("cmdstat_") and "ft." in key.lower():
                command = key[8:]  # Remove "cmdstat_" prefix
                logging.info(f"  {command}: {value}")

        # Latency statistics
        latency_stats = await client.info("latencystats")
        logging.info("Latency Statistics:")
        for key, value in latency_stats.items():
            if "ft." in key.lower():
                logging.info(f"  {key}: {value}")

        # Memory statistics
        memory_stats = await client.info("memory")
        logging.info("Memory Statistics:")
        important_memory_keys = [
            "used_memory",
            "used_memory_human",
            "used_memory_rss",
            "used_memory_rss_human",
            "used_memory_peak",
            "used_memory_peak_human",
        ]
        for key in important_memory_keys:
            if key in memory_stats:
                logging.info(f"  {key}: {memory_stats[key]}")

        logging.info(f"{test_name} completed successfully")

        # Close client
        await client.aclose()

    async def test_standard_benchmark(self, df_server: DflyInstance, prepared_benchmark_data):
        """Standard benchmark test - 100 queries with 10 concurrent clients."""
        await self._run_benchmark(df_server, prepared_benchmark_data, 100, 10, "Standard Benchmark")

    async def test_small_benchmark(self, df_server: DflyInstance, prepared_benchmark_data):
        """Small benchmark test - 50 queries with 5 concurrent clients."""
        await self._run_benchmark(df_server, prepared_benchmark_data, 50, 5, "Small Benchmark")
