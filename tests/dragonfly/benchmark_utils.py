import asyncio
import logging
import random
import string
import uuid
import math
from typing import Dict, List
from redis import asyncio as aioredis
from redis.commands.search.query import Query


def set_random_seed(seed: int = 42):
    random.seed(seed)


class DragonFlyColumnTypes:
    NUMERIC = "NUMERIC"
    TAG = "TAG"
    TEXT = "TEXT"


class ServerTypes:
    UNIQUE_IDENTIFIER = "uniqueidentifier"
    INT = "int"
    BIT = "bit"
    NVARCHAR = "nvarchar"
    MONEY = "money"
    N_TEXT = "ntext"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    DECIMAL = "decimal"
    FLOAT = "float"
    BIGINT = "bigint"
    IMAGE = "image"


INDEX_KEY = "idx:AccountBase"
ACCOUNT_KEY = "AccountBase:{accountId}"


def generate_account_columns(num_columns: int = 1024) -> List[Dict[str, str]]:
    max_text_fields = 128

    server_types = [
        ServerTypes.NVARCHAR,
        ServerTypes.INT,
        ServerTypes.BIT,
        ServerTypes.UNIQUE_IDENTIFIER,
        ServerTypes.MONEY,
        ServerTypes.DATETIME,
        ServerTypes.FLOAT,
        ServerTypes.BIGINT,
    ]

    columns = []
    # Track generated names to guarantee uniqueness
    existing_names: set[str] = set()
    text_field_count = 0

    # Add some standard columns
    standard_columns = [
        {"COLUMN_NAME": "AccountId", "TYPE_NAME": ServerTypes.UNIQUE_IDENTIFIER},
        {"COLUMN_NAME": "Name", "TYPE_NAME": ServerTypes.NVARCHAR},
        {"COLUMN_NAME": "AccountNumber", "TYPE_NAME": ServerTypes.NVARCHAR},
        {"COLUMN_NAME": "Revenue", "TYPE_NAME": ServerTypes.MONEY},
        {"COLUMN_NAME": "NumberOfEmployees", "TYPE_NAME": ServerTypes.INT},
        {"COLUMN_NAME": "CreatedOn", "TYPE_NAME": ServerTypes.DATETIME},
        {"COLUMN_NAME": "ModifiedOn", "TYPE_NAME": ServerTypes.DATETIME},
        {"COLUMN_NAME": "IsPrivate", "TYPE_NAME": ServerTypes.BIT},
        {"COLUMN_NAME": "StateCode", "TYPE_NAME": ServerTypes.INT},
        {"COLUMN_NAME": "StatusCode", "TYPE_NAME": ServerTypes.INT},
    ]

    columns.extend(standard_columns)
    existing_names.update(col["COLUMN_NAME"] for col in standard_columns)

    for col in standard_columns:
        if col["TYPE_NAME"] in [ServerTypes.NVARCHAR, ServerTypes.N_TEXT]:
            text_field_count += 1

    while len(columns) < num_columns:
        candidate_name = (
            f"lv_{''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 15)))}"
        )

        # Ensure the column name is unique
        if candidate_name in existing_names:
            continue

        if text_field_count > max_text_fields - 1:
            non_text_types = [
                t for t in server_types if t not in [ServerTypes.NVARCHAR, ServerTypes.N_TEXT]
            ]
            column_type = random.choice(non_text_types)
        else:
            column_type = random.choice(server_types)
            if column_type in [ServerTypes.NVARCHAR, ServerTypes.N_TEXT]:
                text_field_count += 1

        columns.append({"COLUMN_NAME": candidate_name, "TYPE_NAME": column_type})
        existing_names.add(candidate_name)

    logging.info(f"Created {len(columns)} columns, with {text_field_count} TEXT fields")

    return columns


def map_server_type_to_dragonfly_type(server_type: str) -> str:
    mapping = {
        ServerTypes.UNIQUE_IDENTIFIER: DragonFlyColumnTypes.TAG,
        ServerTypes.INT: DragonFlyColumnTypes.NUMERIC,
        ServerTypes.BIT: DragonFlyColumnTypes.NUMERIC,
        ServerTypes.NVARCHAR: DragonFlyColumnTypes.TEXT,
        ServerTypes.MONEY: DragonFlyColumnTypes.NUMERIC,
        ServerTypes.N_TEXT: DragonFlyColumnTypes.TEXT,
        ServerTypes.DATETIME: DragonFlyColumnTypes.NUMERIC,
        ServerTypes.TIMESTAMP: DragonFlyColumnTypes.TEXT,
        ServerTypes.DECIMAL: DragonFlyColumnTypes.NUMERIC,
        ServerTypes.FLOAT: DragonFlyColumnTypes.NUMERIC,
        ServerTypes.BIGINT: DragonFlyColumnTypes.NUMERIC,
        ServerTypes.IMAGE: DragonFlyColumnTypes.TEXT,
    }

    if server_type not in mapping:
        raise ValueError(f"Unknown Server type: {server_type}")

    return mapping[server_type]


async def create_search_index(client: aioredis.Redis, columns: List[Dict[str, str]]) -> None:
    # Map columns to Dragonfly types
    column_name_to_dragonfly_type = {}
    text_field_count = 0

    for column in columns:
        dragonfly_type = map_server_type_to_dragonfly_type(column["TYPE_NAME"])
        column_name_to_dragonfly_type[column["COLUMN_NAME"]] = dragonfly_type

        if dragonfly_type == DragonFlyColumnTypes.TEXT:
            text_field_count += 1

    # Check TEXT field limit
    if text_field_count > 128:
        raise ValueError(
            f"Too many TEXT fields: {text_field_count}. RediSearch supports a maximum of 128 TEXT fields."
        )

    logging.info(
        f"Creating index with {len(columns)} columns, including {text_field_count} TEXT fields"
    )

    # Build schema command
    dragonfly_columns = " ".join(
        [
            f"{column_name} {dragonfly_type}"
            for column_name, dragonfly_type in column_name_to_dragonfly_type.items()
        ]
    )

    schema_create_command = (
        f"FT.CREATE {INDEX_KEY} ON HASH PREFIX 1 AccountBase: SCHEMA {dragonfly_columns}"
    )
    await client.execute_command(schema_create_command)


def get_column_name_to_type_mapping(columns: List[Dict[str, str]]) -> Dict[str, str]:
    return {column["COLUMN_NAME"]: column["TYPE_NAME"] for column in columns}


PRE_GENERATED_STRINGS = []
PRE_GENERATED_UIDS = []


def _initialize_pre_generated_data(size: int):
    global PRE_GENERATED_STRINGS, PRE_GENERATED_UIDS

    # Clear previous data and generate new
    PRE_GENERATED_STRINGS.clear()
    PRE_GENERATED_UIDS.clear()

    PRE_GENERATED_STRINGS.extend(
        [
            "".join(random.choices(string.ascii_letters, k=k))
            for _ in range(size)
            for k in range(5, 11)  # lengths 5–10
        ]
    )

    PRE_GENERATED_UIDS.extend([str(uuid.uuid4()) for _ in range(size)])


def generate_property_value(column_type: str):
    if column_type in [ServerTypes.NVARCHAR, ServerTypes.N_TEXT]:
        return random.choice(PRE_GENERATED_STRINGS)
    elif column_type in [
        ServerTypes.INT,
        ServerTypes.MONEY,
        ServerTypes.DECIMAL,
        ServerTypes.FLOAT,
        ServerTypes.DATETIME,
        ServerTypes.BIGINT,
    ]:
        return random.randint(1, 100)
    elif column_type == ServerTypes.BIT:
        return random.choice([0, 1])
    elif column_type == ServerTypes.UNIQUE_IDENTIFIER:
        return random.choice(PRE_GENERATED_UIDS)
    elif column_type == ServerTypes.TIMESTAMP:
        return None
    elif column_type == ServerTypes.IMAGE:
        return None
    else:
        raise NotImplementedError(
            f"Type {column_type} is not implemented in generate_property_value"
        )


async def generate_account_data(
    client: aioredis.Redis,
    column_mappings: Dict[str, str],
    num_accounts: int = 10000,  # Default for quick testing
    chunk_size: int = 1000,  # Chunk size for batch processing
) -> List[str]:
    # Initialize pre-generated data with required size
    _initialize_pre_generated_data(num_accounts)

    # Generate account IDs
    account_ids = [str(uuid.uuid4()) for _ in range(num_accounts)]

    # Process accounts in chunks for better performance
    chunks_count = math.ceil(num_accounts / chunk_size)

    tasks = []
    for chunk_number in range(chunks_count):
        start_idx = chunk_number * chunk_size
        end_idx = min((chunk_number + 1) * chunk_size, num_accounts)
        chunk_account_ids = account_ids[start_idx:end_idx]

        task = asyncio.create_task(
            _generate_accounts_chunk(client, chunk_account_ids, column_mappings)
        )
        tasks.append(task)

    # Wait for all chunks to complete
    await asyncio.gather(*tasks)

    return account_ids


async def _generate_accounts_chunk(
    client: aioredis.Redis, account_ids: List[str], column_mappings: Dict[str, str]
):
    pipeline = client.pipeline()

    for account_id in account_ids:
        account = {"AccountId": account_id}

        # Generate values for all columns except AccountId
        for column_name, column_type in column_mappings.items():
            if column_name == "AccountId":
                continue

            value = generate_property_value(column_type)
            if value is not None:
                account[column_name] = value

        acc_key = ACCOUNT_KEY.format(accountId=account_id)
        pipeline.hset(acc_key, mapping=account)

    await pipeline.execute()


def create_property_filter(property_name: str, property_type: str, account_ids: List[str]) -> str:
    # Return None for types that cannot be filtered
    if property_type in [ServerTypes.TIMESTAMP, ServerTypes.IMAGE]:
        return None

    if property_type in [ServerTypes.NVARCHAR, ServerTypes.N_TEXT]:
        filter_string = "".join(random.choices(string.ascii_letters, k=3))
        return f'@{property_name}: "*{filter_string}*"'
    elif property_type in [
        ServerTypes.INT,
        ServerTypes.MONEY,
        ServerTypes.DECIMAL,
        ServerTypes.FLOAT,
        ServerTypes.DATETIME,
        ServerTypes.BIGINT,
    ]:
        filter_number = random.randint(1, 100)
        return f"@{property_name}: [{filter_number} +inf]"
    elif property_type == ServerTypes.BIT:
        filter_number = random.choice([0, 1])
        return f"@{property_name}: [{filter_number} {filter_number}]"
    elif property_type == ServerTypes.UNIQUE_IDENTIFIER:
        filter_string = random.choice(account_ids).replace("-", "\\-")
        return f"@{property_name}: {{{filter_string}}}"
    else:
        raise NotImplementedError(
            f"Type {property_type} is not implemented in create_property_filter"
        )


def generate_search_query(column_mappings: Dict[str, str], account_ids: List[str]) -> Query:
    columns = list(column_mappings.keys())

    if random.random() < 0.1:
        num_columns = random.randint(1, min(500, len(columns)))
        selected_columns = random.sample(columns, num_columns)
        filter_string = "*"

        query = Query(filter_string).return_fields(*selected_columns)
        query = query.paging(0, 50)
        return query

    reliable_filter_columns = [
        col
        for col, col_type in column_mappings.items()
        if col_type in [ServerTypes.INT, ServerTypes.BIT, ServerTypes.MONEY]
    ]

    num_columns = random.randint(1, min(500, len(columns)))
    selected_columns = random.sample(columns, num_columns)

    if reliable_filter_columns and random.random() < 0.5:
        filter_column = random.choice(reliable_filter_columns)
        filter_str = create_simple_numeric_filter(filter_column, column_mappings[filter_column])
        filter_string = filter_str if filter_str else "*"
    else:
        filter_string = "*"

    query = Query(filter_string).return_fields(*selected_columns)
    query = query.paging(0, 50)

    return query


def create_simple_numeric_filter(property_name: str, property_type: str) -> str:
    if property_type == ServerTypes.INT:
        return f"@{property_name}: [1 100]"
    elif property_type == ServerTypes.BIT:
        bit_value = random.choice([0, 1])
        return f"@{property_name}: [{bit_value} {bit_value}]"
    elif property_type == ServerTypes.MONEY:
        return f"@{property_name}: [1 1000]"
    else:
        return "*"


async def run_query_agent(
    agent_id: int,
    df_server,
    column_mappings: Dict[str, str],
    account_ids: List[str],
    num_queries: int,
) -> int:
    # Create a dedicated client for this agent to avoid contention
    client = df_server.client()

    query_count = 0
    success_count = 0

    try:
        for i in range(num_queries):
            try:
                query = generate_search_query(column_mappings, account_ids)
                results = await client.ft(INDEX_KEY).search(query)
                success_count += 1

            except Exception as e:
                logging.error(f"Agent {agent_id}: ERROR in query {i}: {e}")

            query_count += 1

    finally:
        if query_count > 0:
            final_success_rate = (success_count / query_count) * 100
            logging.info(
                f"Agent {agent_id} completed: {success_count}/{query_count} successful queries ({final_success_rate:.1f}%)"
            )
        await client.aclose()

    return success_count


async def run_query_load_test(
    df_server,
    column_mappings: Dict[str, str],
    account_ids: List[str],
    total_queries: int = 1000,
    num_agents: int = 50,
) -> int:
    queries_per_agent = total_queries // num_agents

    tasks = []
    for agent_id in range(num_agents):
        task = asyncio.create_task(
            run_query_agent(agent_id, df_server, column_mappings, account_ids, queries_per_agent)
        )
        tasks.append(task)

    # Wait for all agents to complete
    results = await asyncio.gather(*tasks)
    total_completed = sum(results)

    return total_completed
