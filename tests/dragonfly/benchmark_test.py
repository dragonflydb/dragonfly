import logging
import time
import pytest

from . import dfly_args
from .instance import DflyInstance
from .benchmark_utils import (
    generate_account_columns,
    create_search_index,
    get_column_name_to_type_mapping,
    generate_account_data,
    run_query_load_test,
    set_random_seed,
    INDEX_KEY,
    ACCOUNT_KEY,
)


async def run_dragonfly_benchmark(
    df_server: DflyInstance,
    num_accounts: int = 200,
    num_queries: int = 500,
    num_agents: int = 50,
    random_seed: int = 42,
):
    set_random_seed(random_seed)

    logging.info(f"Starting Dragonfly benchmark test on port {df_server.port}")
    logging.info(
        f"Parameters: {num_accounts} accounts, {num_queries} queries, {num_agents} agents, seed={random_seed}"
    )
    client = df_server.client()

    # Basic connectivity check
    assert await client.ping() == True

    # Stage 1: Schema Generation
    logging.info("Stage 1: Schema Generation - generating columns and creating search index")
    account_columns = generate_account_columns()
    await create_search_index(client, account_columns)

    # Verify the index was created
    index_info = await client.execute_command(f"FT.INFO {INDEX_KEY}")
    assert index_info is not None
    logging.info(
        f"Stage 1 completed: search index '{INDEX_KEY}' created with {len(account_columns)} columns"
    )

    # Get column mappings for future stages
    column_mappings = get_column_name_to_type_mapping(account_columns)
    assert len(column_mappings) == len(account_columns)

    # Stage 2: Data Generation
    logging.info(
        f"Stage 2: Data Generation - generating {num_accounts:,} accounts with full column data"
    )
    stage2_start = time.perf_counter()
    account_ids = await generate_account_data(
        client=client,
        column_mappings=column_mappings,
        num_accounts=num_accounts,
        chunk_size=1000,  # Chunk size for batch processing
    )

    # Verify data was generated
    assert len(account_ids) == num_accounts

    # Verify some accounts were stored
    sample_account_id = account_ids[0]
    account_key = ACCOUNT_KEY.format(accountId=sample_account_id)
    stored_account = await client.hgetall(account_key)
    assert stored_account is not None
    assert stored_account["AccountId"] == sample_account_id
    stage2_duration = time.perf_counter() - stage2_start
    logging.info(
        f"Stage 2 completed in {stage2_duration:.2f}s: {len(account_ids)} accounts generated and stored"
    )

    # Stage 3: Query Load Testing
    logging.info(
        f"Stage 3: Query Load Testing - running {num_queries:,} queries with {num_agents} concurrent agents"
    )
    stage3_start = time.perf_counter()
    total_completed = await run_query_load_test(
        df_server=df_server,
        column_mappings=column_mappings,
        account_ids=account_ids,
        total_queries=num_queries,
        num_agents=num_agents,
    )

    # Verify queries completed
    assert total_completed > 0
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
    await run_dragonfly_benchmark(df_server, 20000, 1000)
