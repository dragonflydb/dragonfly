import random
import string
import uuid
import math
from typing import Dict, List
import redis


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
ACCOUNT_REDIS_KEY = "AccountBase:{accountId}"


def generate_account_columns(num_columns: int = 2774) -> List[Dict[str, str]]:
    sql_types = [
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

    # Generate additional random columns
    for i in range(num_columns - len(standard_columns)):
        column_name = (
            f"lv_{''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 15)))}"
        )
        column_type = random.choice(sql_types)
        columns.append({"COLUMN_NAME": column_name, "TYPE_NAME": column_type})

    return columns


def map_sql_type_to_dragonfly_type(sql_type: str) -> str:
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

    if sql_type not in mapping:
        raise ValueError(f"Unknown Server type: {sql_type}")

    return mapping[sql_type]


def create_search_index(client: redis.Redis, columns: List[Dict[str, str]]) -> None:
    # Map columns to Dragonfly types
    column_name_to_dragonfly_type = {}
    for column in columns:
        dragonfly_type = map_sql_type_to_dragonfly_type(column["TYPE_NAME"])
        column_name_to_dragonfly_type[column["COLUMN_NAME"]] = dragonfly_type

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
    client.execute_command(schema_create_command)


def get_column_name_to_type_mapping(columns: List[Dict[str, str]]) -> Dict[str, str]:
    return {column["COLUMN_NAME"]: column["TYPE_NAME"] for column in columns}


# Pre-generated data for performance
PRE_GENERATED_STRINGS = [
    "".join(random.choices(string.ascii_letters, k=k))
    for _ in range(10000)  # Original size
    for k in range(5, 11)  # lengths 5–10
]

PRE_GENERATED_UIDS = [str(uuid.uuid4()) for _ in range(10000)]  # Original size


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


def generate_account_data(
    client: redis.Redis,
    column_mappings: Dict[str, str],
    num_accounts: int = 10000,
    chunk_size: int = 500,
) -> List[str]:
    # Generate account IDs
    account_ids = [str(uuid.uuid4()) for _ in range(num_accounts)]

    # Process accounts in chunks for better performance
    chunks_count = math.ceil(num_accounts / chunk_size)

    for chunk_number in range(chunks_count):
        start_idx = chunk_number * chunk_size
        end_idx = min((chunk_number + 1) * chunk_size, num_accounts)
        chunk_account_ids = account_ids[start_idx:end_idx]

        _generate_accounts_chunk(client, chunk_account_ids, column_mappings)

    return account_ids


def _generate_accounts_chunk(
    client: redis.Redis, account_ids: List[str], column_mappings: Dict[str, str]
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

        redis_key = ACCOUNT_REDIS_KEY.format(accountId=account_id)
        pipeline.hset(redis_key, mapping=account)

    pipeline.execute()


# TODO: Add query generation functions (account_queries.py)
# TODO: Add update functions (account_updates.py)
# TODO: Add performance metrics collection
