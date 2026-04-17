from dataclasses import dataclass
from secrets import token_bytes
from typing import Any

from native_dumper import HTTPCursor
from psycopg import Cursor

from ..core import (
    clickhouse_ddl,
    postgres_ddl,
)
from . import errors


SERVER_NAME = {
    HTTPCursor: clickhouse_ddl,
    Cursor: postgres_ddl,
}

@dataclass
class ETLInfo:
    """Defines for ETL operations."""

    name: str
    ddl: str
    transit_table: str
    transit_ddl: str
    columns: list[dict[str, Any]]


def __validate_ddl(table_meta: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate DDL data."""

    columns: list[dict[str, Any]] = table_meta.get("columns", [])

    if not columns:
        raise errors.DBHoseAirflowValueError(
            "No columns found in table metadata",
        )

    return columns


def __build_postgres_transit_ddl(
    transit_table: str,
    table_meta: dict[str, Any],
) -> tuple[str, list[dict[str, Any]]]:
    """Build UNLOGGED transit table DDL for PostgreSQL/Greenplum."""

    columns = __validate_ddl(table_meta)
    col_defs = []

    for col in columns:
        col_name = f'"{col["attname"]}"'
        col_type = col["typname"]

        if col.get("attnotnull"):
            col_def = f"    {col_name} {col_type} NOT NULL"
        else:
            col_def = f"    {col_name} {col_type}"

        col_defs.append(col_def)

    columns_str = ",\n".join(col_defs)
    ddl = (
        f"CREATE UNLOGGED TABLE IF NOT EXISTS {transit_table} (\n"
        f"{columns_str}\n"
        f") WITH (autovacuum_enabled = false)"
    )
    distkey = table_meta.get("distkey")

    if distkey:
        quoted_cols = [f'"{c}"' for c in distkey]
        ddl += f"\nDISTRIBUTED BY ({', '.join(quoted_cols)})"

    partition_key = table_meta.get("partition_key")

    if partition_key:
        ddl += f"\nPARTITION BY {partition_key}"

    return ddl, columns


def __build_clickhouse_transit_ddl(
    transit_table: str, table_meta: dict[str, Any]
) -> tuple[str, list[dict[str, Any]]]:
    """Build transit table DDL for ClickHouse with MergeTree engine."""

    columns = __validate_ddl(table_meta)
    col_defs = []

    for col in columns:
        col_name = f"`{col['name']}`"
        col_type = col["data_type"]
        col_defs.append(f"    {col_name} {col_type}")

    columns_str = ",\n".join(col_defs)
    order_by = table_meta.get("order_by")

    if order_by:
        order_by_str = ", ".join(f"`{c}`" for c in order_by)
    else:
        order_by_str = f"`{columns[0]['name']}`"

    partition_by = table_meta.get("partition_by")
    partition_clause = f"\nPARTITION BY {partition_by}" if partition_by else ""
    settings = table_meta.get("settings") or {}
    settings["allow_suspicious_low_cardinality_types"] = "1"
    settings_str = ", ".join(f"{k} = {v}" for k, v in settings.items())

    return (
        f"CREATE TABLE IF NOT EXISTS {transit_table} (\n"
        f"{columns_str}\n"
        f")\n"
        f"ENGINE = MergeTree{partition_clause}\n"
        f"ORDER BY ({order_by_str})\n"
        f"SETTINGS {settings_str}"
    ), columns


def build_transit_ddl(
    transit_table: str,
    table_meta: dict[str, Any],
    is_postgres: bool,
) -> tuple[str, list[dict[str, Any]]]:
    """Generate DDL for transit table."""

    if is_postgres:
        return __build_postgres_transit_ddl(transit_table, table_meta)

    return __build_clickhouse_transit_ddl(transit_table, table_meta)


def generate_ddl(
    table_name: str,
    cursor: Cursor | HTTPCursor,
    random_prefix: bool = True,
) -> tuple[str, ...]:
    """Generate DDLs and transit table."""

    ddl_core = SERVER_NAME.get(cursor.__class__)

    if not ddl_core:
        raise errors.DBHoseAirflowTypeError("No DDL method found.")

    if table_name[-1] in ["`", '"']:
        _table_name = table_name[:-1]
        _closing = table_name[-1]
    else:
        _table_name = table_name
        _closing = ""

    transit_table = f"{_table_name}_temp"

    if random_prefix:
        transit_table += token_bytes(4).hex()

    transit_table += _closing

    try:
        table_ddl, table_meta = ddl_core(cursor, table_name)
        transit_ddl, columns = build_transit_ddl(
            transit_table,
            table_meta,
            type(cursor) is Cursor,
        )
        return ETLInfo(
            table_name,
            table_ddl,
            transit_table,
            transit_ddl,
            columns,
        )
    except Exception as error:
        raise errors.DBHoseAirflowError(error)
