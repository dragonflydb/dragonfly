"""
Benchmark utilities for Dragonfly performance testing.
Replicates functionality from original POC scripts:
- schema_generator.py
- account_data_generator.py
- account_queries.py
- account_updates.py
"""

import random
import string
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


# Constants
INDEX_KEY = "idx:AccountBase"
ACCOUNT_REDIS_KEY = "AccountBase:{accountId}"


def generate_account_columns(num_columns: int = 100) -> List[Dict[str, str]]:
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
    """
    Map Server types to Dragonfly types
    """
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


# TODO: Add data generation functions (account_data_generator.py)
# TODO: Add query generation functions (account_queries.py)
# TODO: Add update functions (account_updates.py)
# TODO: Add performance metrics collection
