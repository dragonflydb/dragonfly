"""
Dragonfly benchmark test
Replicates the workflow from original POC scripts in a single pytest test:
1. schema_generator.py - Create search index
2. account_data_generator.py - Generate test data
3. account_queries.py - Run concurrent queries
4. account_updates.py - Run concurrent updates
"""

import redis

from . import dfly_args
from .instance import DflyInstance
from .benchmark_utils import (
    generate_account_columns,
    create_search_index,
    get_column_name_to_type_mapping,
    INDEX_KEY,
)


@dfly_args({"proactor_threads": 4})
def test_dragonfly_benchmark(df_server: DflyInstance):
    """
    Comprehensive Dragonfly benchmark test
    Gradually implements all POC functionality in stages
    """
    # Create redis client from df_server fixture
    client = redis.Redis(port=df_server.port)

    # Basic connectivity check
    assert client.ping() == True

    # Stage 1: Schema Generation (replaces schema_generator.py)
    account_columns = generate_account_columns(num_columns=50)
    create_search_index(client, account_columns)

    # Verify the index was created
    index_info = client.execute_command(f"FT.INFO {INDEX_KEY}")
    assert index_info is not None

    # Get column mappings for future stages
    column_mappings = get_column_name_to_type_mapping(account_columns)
    assert len(column_mappings) == len(account_columns)

    # TODO: Stage 2 - Data Generation (account_data_generator.py)
    # TODO: Stage 3 - Load Testing (account_queries.py + account_updates.py)
    # TODO: Stage 4 - Results & Cleanup
