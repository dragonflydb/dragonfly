import redis

from . import dfly_args
from .instance import DflyInstance
from .benchmark_utils import (
    generate_account_columns,
    create_search_index,
    get_column_name_to_type_mapping,
    generate_account_data,
    INDEX_KEY,
)


@dfly_args({"proactor_threads": 4})
def test_dragonfly_benchmark(df_server: DflyInstance):
    client = redis.Redis(port=df_server.port)

    # Basic connectivity check
    assert client.ping() == True

    # Stage 1: Schema Generation
    account_columns = generate_account_columns()  # Uses default 2774 columns like original
    create_search_index(client, account_columns)

    # Verify the index was created
    index_info = client.execute_command(f"FT.INFO {INDEX_KEY}")
    assert index_info is not None

    # Get column mappings for future stages
    column_mappings = get_column_name_to_type_mapping(account_columns)
    assert len(column_mappings) == len(account_columns)

    # Stage 2: Data Generation
    account_ids = generate_account_data(
        client=client,
        column_mappings=column_mappings,
        num_accounts=1000,  # Original size
        chunk_size=1000,  # Efficient chunk size for large dataset
    )

    # Verify data was generated
    assert len(account_ids) == 1000

    # Verify some accounts were stored
    sample_account_id = account_ids[0]
    account_key = f"AccountBase:{sample_account_id}"
    stored_account = client.hgetall(account_key)
    assert stored_account is not None
    assert stored_account[b"AccountId"].decode() == sample_account_id

    # TODO: Stage 3 - Load Testing (account_queries.py + account_updates.py)
    # TODO: Stage 4 - Results & Cleanup
