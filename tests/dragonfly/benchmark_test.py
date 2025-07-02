import logging
import redis

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
)


def run_dragonfly_benchmark(
    df_server: DflyInstance,
    num_accounts: int = 10000,
    num_queries: int = 1000,
    num_agents: int = 50,
    random_seed: int = 42,
):
    set_random_seed(random_seed)

    logging.info(f"Starting Dragonfly benchmark test on port {df_server.port}")
    logging.info(
        f"Parameters: {num_accounts} accounts, {num_queries} queries, {num_agents} agents, seed={random_seed}"
    )
    client = redis.Redis(port=df_server.port)

    # Basic connectivity check
    assert client.ping() == True

    # Stage 1: Schema Generation
    logging.info("Stage 1: Schema Generation - generating columns and creating search index")
    account_columns = generate_account_columns()
    create_search_index(client, account_columns)

    # Verify the index was created
    index_info = client.execute_command(f"FT.INFO {INDEX_KEY}")
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
    account_ids = generate_account_data(
        client=client,
        column_mappings=column_mappings,
        num_accounts=num_accounts,
        chunk_size=1000,  # Chunk size for batch processing
    )

    # Verify data was generated
    assert len(account_ids) == num_accounts

    # Verify some accounts were stored
    sample_account_id = account_ids[0]
    account_key = f"AccountBase:{sample_account_id}"
    stored_account = client.hgetall(account_key)
    assert stored_account is not None
    assert stored_account[b"AccountId"].decode() == sample_account_id
    logging.info(f"Stage 2 completed: {len(account_ids)} accounts generated and stored in Redis")

    # Stage 3: Query Load Testing
    logging.info(
        f"Stage 3: Query Load Testing - running {num_queries:,} queries with {num_agents} concurrent agents"
    )
    total_completed = run_query_load_test(
        client=client,
        column_mappings=column_mappings,
        account_ids=account_ids,
        total_queries=num_queries,
        num_agents=num_agents,
    )

    # Verify queries completed
    assert total_completed > 0
    logging.info(f"Stage 3 completed: {total_completed} queries executed successfully")

    logging.info("Benchmark test completed successfully")


@dfly_args({"proactor_threads": 4})
def test_dragonfly_benchmark_small(df_server: DflyInstance):
    run_dragonfly_benchmark(df_server)
