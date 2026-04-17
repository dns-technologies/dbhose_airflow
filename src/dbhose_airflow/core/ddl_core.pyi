from typing import Any

from native_dumper import HTTPCursor
from psycopg import Cursor


def clickhouse_ddl(
    cursor: HTTPCursor,
    object_name: str,
) -> tuple[str, dict[str, Any]]:
    """
    Retrieve the DDL and metadata for a ClickHouse object.

    Args:
        cursor: An HTTPCursor with an active connection
        object_name: The full name of the object (e.g., "default.my_table")

    Returns:
        A tuple (ddl: str, metadata: dict)

    """
    ...


def postgres_ddl(
    cursor: Cursor,
    object_name: str,
    options: dict[str, bool] | None = None,
) -> tuple[str, dict[str, Any]]:
    """
    Retrieve the DDL and metadata for a PostgreSQL/Greenplum object.

    Args:
        cursor: A psycopg cursor with an active connection
        object_name: The full name of the object (e.g., "public.my_table")
        options: An optional dictionary of flags:
            - include_indexes: bool (default True)
            - include_constraints_fk: bool (default True)
            - include_constraints_check: bool (default True)
            - include_owner: bool (default True)
            - include_comments: bool (default True)
            - include_acl: bool (default True)
            - include_distributed_by: bool (default True)
            - include_partitions: bool (default True)
            - include_triggers: bool (default True)

    Returns:
        Tuple (ddl: str, metadata: dict)

    """
    ...


def postgres_sequence_ddl(
    cursor: Cursor,
    object_name: str,
) -> tuple[str, dict[str, Any]]:
    """
    Retrieve the DDL and metadata for a PostgreSQL sequence.

    Args:
        cursor: A psycopg cursor with an active connection
        object_name: The full name of the sequence

    Returns:
        A tuple (ddl: str, metadata: dict)

    """
    ...
