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


async def run_dragonfly_benchmark(
    df_server: DflyInstance,
    num_documents: int,
    num_queries: int,
    num_agents: int,
    random_seed: int,
):
    set_random_seed(random_seed)

    logging.info(f"Starting Dragonfly benchmark test on port {df_server.port}")
    logging.info(
        f"Parameters: {num_documents} documents, {num_queries} queries, {num_agents} agents, seed={random_seed}"
    )
    client = df_server.client()

    # Basic connectivity check
    assert await client.ping() == True

    # Stage 1: Schema Generation
    logging.info("Stage 1: Schema Generation - generating columns and creating search index")
    document_columns = generate_document_columns()
    await create_search_index(client, document_columns)

    # Verify the index was created
    index_info = await client.execute_command(f"FT.INFO {INDEX_KEY}")
    assert index_info is not None
    logging.info(
        f"Stage 1 completed: search index '{INDEX_KEY}' created with {len(document_columns)} columns"
    )

    # Stage 2: Data Generation
    logging.info(
        f"Stage 2: Data Generation - generating {num_documents:,} documents with full column data"
    )
    stage2_start = time.perf_counter()
    document_ids = await generate_document_data(
        client=client,
        columns=document_columns,
        num_documents=num_documents,
        chunk_size=1000,  # Chunk size for batch processing
    )

    # Verify data was generated
    assert len(document_ids) == num_documents

    # Verify some documents were stored
    sample_document_id = document_ids[0]
    document_key = DOCUMENT_KEY.format(documentId=sample_document_id)
    stored_document = await client.hgetall(document_key)
    assert stored_document is not None
    assert stored_document["DocumentId"] == sample_document_id
    stage2_duration = time.perf_counter() - stage2_start
    logging.info(
        f"Stage 2 completed in {stage2_duration:.2f}s: {len(document_ids)} documents generated and stored"
    )

    # Stage 3: Query Load Testing
    logging.info(
        f"Stage 3: Query Load Testing - running {num_queries:,} queries with {num_agents} concurrent agents"
    )
    stage3_start = time.perf_counter()
    total_completed = await run_query_load_test(
        df_server=df_server,
        columns=document_columns,
        document_ids=document_ids,
        total_queries=num_queries,
        num_agents=num_agents,
    )

    # Verify queries completed
    assert total_completed == num_queries
    stage3_duration = time.perf_counter() - stage3_start
    logging.info(
        f"Stage 3 completed in {stage3_duration:.2f}s: {total_completed} queries executed successfully"
    )

    # Final summary
    logging.info(
        f"Benchmark Timings Summary -> Data Generation: {stage2_duration:.2f}s | Query Load: {stage3_duration:.2f}s"
    )

    logging.info("Benchmark test completed successfully")

    # Close client
    await client.aclose()


@dfly_args({"proactor_threads": 4})
@pytest.mark.opt_only
@pytest.mark.slow
async def test_dragonfly_benchmark(
    df_server: DflyInstance,
):
    await run_dragonfly_benchmark(df_server, 5000, 200, 25, 42)
