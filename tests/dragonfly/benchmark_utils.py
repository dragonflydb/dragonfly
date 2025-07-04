import asyncio
import logging
import random
import string
import uuid
import math
from typing import Dict, List, Tuple
from redis import asyncio as aioredis
from redis.commands.search.query import Query


def set_random_seed(seed: int = 42):
    random.seed(seed)


INDEX_KEY = "idx:AccountBase"
ACCOUNT_KEY = "AccountBase:{accountId}"


# Simple data types for generation
COLUMN_TYPES = {
    "TEXT": {
        "dragonfly_type": "TEXT",
        "generator": lambda: random.choice(PRE_GENERATED_STRINGS),
    },
    "NUMERIC": {
        "dragonfly_type": "NUMERIC",
        "generator": lambda: random.randint(1, 100),
    },
    "TAG": {
        "dragonfly_type": "TAG",
        "generator": lambda: random.choice(PRE_GENERATED_UIDS),
    },
    "BIT": {
        "dragonfly_type": "NUMERIC",
        "generator": lambda: random.choice([0, 1]),
    },
}


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


async def generate_account_data(
    client: aioredis.Redis,
    columns: List[Tuple[str, str]],
    num_accounts: int = 10000,
    chunk_size: int = 1000,
) -> List[str]:
    # Initialize pre-generated data
    _initialize_pre_generated_data(num_accounts)

    # Generate account IDs
    account_ids = [str(uuid.uuid4()) for _ in range(num_accounts)]

    # Process in chunks for better performance
    chunks_count = math.ceil(num_accounts / chunk_size)

    tasks = []
    for chunk_number in range(chunks_count):
        start_idx = chunk_number * chunk_size
        end_idx = min((chunk_number + 1) * chunk_size, num_accounts)
        chunk_account_ids = account_ids[start_idx:end_idx]

        task = asyncio.create_task(_generate_accounts_chunk(client, chunk_account_ids, columns))
        tasks.append(task)

    await asyncio.gather(*tasks)
    return account_ids


async def _generate_accounts_chunk(
    client: aioredis.Redis, account_ids: List[str], columns: List[Tuple[str, str]]
):
    pipeline = client.pipeline()

    for account_id in account_ids:
        account = {"AccountId": account_id}

        # Generate values for all columns except AccountId
        for column_name, column_type in columns:
            if column_name == "AccountId":
                continue

            value = COLUMN_TYPES[column_type]["generator"]()
            if value is not None:
                account[column_name] = value

        acc_key = ACCOUNT_KEY.format(accountId=account_id)
        pipeline.hset(acc_key, mapping=account)

    await pipeline.execute()


def generate_search_query(columns: List[Tuple[str, str]], account_ids: List[str]) -> Query:
    column_names = [name for name, _ in columns]

    if random.random() < 0.5:
        num_columns = random.randint(len(column_names) // 2, len(column_names))
        selected_columns = random.sample(column_names, num_columns)

        query = Query("*").return_fields(*selected_columns)
        query = query.paging(0, 50)
        return query

    reliable_filter_columns = [name for name, col_type in columns if col_type in ["NUMERIC", "BIT"]]

    num_columns = random.randint(len(column_names) // 2, len(column_names))
    selected_columns = random.sample(column_names, num_columns)

    if reliable_filter_columns and random.random() < 0.5:
        filter_column = random.choice(reliable_filter_columns)
        filter_column_type = next(col_type for name, col_type in columns if name == filter_column)
        filter_str = create_simple_numeric_filter(filter_column, filter_column_type)
        filter_string = filter_str if filter_str else "*"
    else:
        filter_string = "*"

    query = Query(filter_string).return_fields(*selected_columns)
    query = query.paging(0, 50)
    return query


def create_simple_numeric_filter(property_name: str, property_type: str) -> str:
    if property_type == "NUMERIC":
        return f"@{property_name}: [1 100]"
    elif property_type == "BIT":
        bit_value = random.choice([0, 1])
        return f"@{property_name}: [{bit_value} {bit_value}]"
    else:
        return "*"


async def run_query_agent(
    agent_id: int,
    df_server,
    columns: List[Tuple[str, str]],
    account_ids: List[str],
    num_queries: int,
) -> int:
    client = df_server.client()

    query_count = 0
    success_count = 0

    try:
        for i in range(num_queries):
            try:
                query = generate_search_query(columns, account_ids)
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
    columns: List[Tuple[str, str]],
    account_ids: List[str],
    total_queries: int = 1000,
    num_agents: int = 50,
) -> int:
    queries_per_agent = total_queries // num_agents

    tasks = []
    for agent_id in range(num_agents):
        task = asyncio.create_task(
            run_query_agent(agent_id, df_server, columns, account_ids, queries_per_agent)
        )
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    total_completed = sum(results)
    return total_completed


def generate_account_columns(num_columns: int = 1024) -> List[Tuple[str, str]]:
    max_text_fields = 128

    # Available types for generation
    available_types = ["TEXT", "NUMERIC", "BIT", "TAG"]

    columns = []
    existing_names = set()
    text_field_count = 0

    # Standard columns
    standard_columns = [
        ("AccountId", "TAG"),
        ("Name", "TEXT"),
        ("AccountNumber", "TEXT"),
        ("Revenue", "NUMERIC"),
        ("NumberOfEmployees", "NUMERIC"),
        ("CreatedOn", "NUMERIC"),
        ("ModifiedOn", "NUMERIC"),
        ("IsPrivate", "BIT"),
        ("StateCode", "NUMERIC"),
        ("StatusCode", "NUMERIC"),
    ]

    columns.extend(standard_columns)
    existing_names.update(name for name, _ in standard_columns)
    text_field_count = sum(1 for _, col_type in standard_columns if col_type == "TEXT")

    while len(columns) < num_columns:
        # Generate unique name
        candidate_name = (
            f"lv_{''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 15)))}"
        )

        if candidate_name in existing_names:
            continue

        # Choose type
        if text_field_count >= max_text_fields:
            column_type = random.choice([t for t in available_types if t != "TEXT"])
        else:
            column_type = random.choice(available_types)
            if column_type == "TEXT":
                text_field_count += 1

        columns.append((candidate_name, column_type))
        existing_names.add(candidate_name)

    logging.info(f"Created {len(columns)} columns, with {text_field_count} TEXT fields")
    return columns


async def create_search_index(client: aioredis.Redis, columns: List[Tuple[str, str]]) -> None:
    text_field_count = sum(1 for _, col_type in columns if col_type == "TEXT")

    if text_field_count > 128:
        raise ValueError(
            f"Too many TEXT fields: {text_field_count}. RediSearch supports a maximum of 128 TEXT fields."
        )

    logging.info(
        f"Creating index with {len(columns)} columns, including {text_field_count} TEXT fields"
    )

    # Create schema directly
    schema_parts = []
    for name, col_type in columns:
        dragonfly_type = COLUMN_TYPES[col_type]["dragonfly_type"]
        schema_parts.append(f"{name} {dragonfly_type}")

    schema_create_command = (
        f"FT.CREATE {INDEX_KEY} ON HASH PREFIX 1 AccountBase: SCHEMA {' '.join(schema_parts)}"
    )
    await client.execute_command(schema_create_command)
