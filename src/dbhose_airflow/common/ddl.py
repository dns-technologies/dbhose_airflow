from secrets import token_bytes
from re import compile
from typing import Any

from native_dumper import HTTPCursor
from nativelib import Column
from psycopg import Cursor

from ..core import (
    clickhouse_ddl,
    postgres_ddl,
)
from . import errors
from .structs import (
    ColumnMeta,
    ETLInfo,
    TableMetadata,
)


PRIMARY_KEY = compile(r"PRIMARY KEY\s*\(([^)]+)\)")
SERVER_NAME = {
    HTTPCursor: clickhouse_ddl,
    Cursor: postgres_ddl,
}


def _normalize_postgres_meta(
    meta: dict[str, list[dict[str, Any]]],
) -> TableMetadata:
    """Normalize PostgreSQL metadata."""

    table_comment = None

    for comment in meta.get("comments", []):
        if comment.get("objsubid") == 0:
            table_comment = comment.get("description")
            break

    primary_key = None

    for constraint in meta.get("constraints", []):
        if constraint.get("contype") == "p":
            pk_def = constraint.get("condef", "")
            match = PRIMARY_KEY.search(pk_def)
            if match:
                primary_key = [
                    c.strip(' \t\n\r"') for c in match.group(1).split(",")
                ]
            break

    engine = meta.get("relkind", "TABLE")
    access_method = meta.get("access_method")
    comments = meta.get("comments", [])
    columns = []

    if access_method and access_method != "heap":
        engine = f"{engine}:{access_method}"

    for num, col in enumerate(meta.get("columns", [])):
        columns.append(ColumnMeta(
                name=col.get("attname"),
                data_type=col.get("typname"),
                nullable=not col.get("attnotnull", False),
                has_default=col.get("atthasdef", False),
                default_value=col.get("defaultval"),
                comment=_find_column_comment(comments, col.get("attnum")),
                position=col.get("attnum", num + 1),
                type_oid=col.get("atttypid"),
                type_namespace=col.get("typnamespace"),
                generated=_pg_generated(col.get("attgenerated")),
                identity=_pg_identity(col.get("attidentity")),
        ))

    return TableMetadata(
        name=meta.get("relname"),
        schema=meta.get("schema_name"),
        owner=meta.get("owner_name"),
        comment=table_comment,
        columns=columns,
        partition_by=meta.get("partition_key"),
        order_by=None,
        primary_key=primary_key,
        engine=engine,
        settings=_parse_reloptions(meta.get("reloptions")),
        oid=meta.get("oid"),
    )


def _normalize_clickhouse_meta(
    meta: dict[str, list[dict[str, Any]]],
) -> TableMetadata:
    """Normalize ClickHouse metadata."""

    engine = meta.get("engine") or "View"
    columns = []

    for num, col in enumerate(meta.get("columns", [])):
        column = Column(col.get("name"), col.get("data_type"))
        is_generated = col.get("default_kind") == "MATERIALIZED"
        generated = "STORED" if is_generated else None
        columns.append(ColumnMeta(
            name=column.column,
            data_type=column.string_dtype,
            nullable=column.info.is_nullable,
            has_default=col.get("default_expr") is not None,
            default_value=col.get("default_expr"),
            comment=col.get("comment"),
            position=num + 1,
            type_oid=None,
            type_namespace=None,
            generated=generated,
            identity=None,
        ))

    return TableMetadata(
        name=meta.get("name"),
        schema=meta.get("database") or "default",
        owner=None,
        comment=meta.get("comment"),
        columns=columns,
        partition_by=meta.get("partition_by"),
        order_by=meta.get("order_by"),
        primary_key=meta.get("primary_key"),
        engine=engine,
        settings=meta.get("settings"),
        oid=None,
    )


def _find_column_comment(
    comments: list[dict[str, Any]],
    attnum: int,
) -> str | None:
    """Find comment for specific column by attnum."""

    for comment in comments:
        if comment.get("objsubid") == attnum:
            return comment.get("description")


def _pg_generated(value: str) -> str | None:
    """Convert PostgreSQL attgenerated to unified format."""

    if value == "s":
        return "STORED"

    if value == "v":
        return "VIRTUAL"


def _pg_identity(value: str) -> str | None:
    """Convert PostgreSQL attidentity to unified format."""

    if value == "a":
        return "ALWAYS"

    if value == "d":
        return "BY DEFAULT"


def _parse_reloptions(options: list[str] | None) -> dict[str, str] | None:
    """Parse PostgreSQL reloptions array to dict."""

    if not options:
        return

    result = {}

    for opt in options:
        if "=" in opt:
            k, v = opt.split("=", 1)
            result[k.strip(" \t\n\r")] = v.strip(" \t\n\r")

    return result


def normalize_metadata(
    table_meta: dict[str, list[dict[str, Any]]],
    is_postgres: bool,
) -> TableMetadata:
    """Convert PostgreSQL or ClickHouse metadata to unified format."""

    if is_postgres:
        return _normalize_postgres_meta(table_meta)

    return _normalize_clickhouse_meta(table_meta)


def __validate_ddl(table_meta: dict[str, Any]) -> list[dict[str, Any]]:
    """Validate DDL data."""

    columns: list[dict[str, Any]] = table_meta.get("columns", [])

    if not columns:
        raise errors.DBHoseAirflowErrorNotFoundError(
            "No columns found in table metadata",
        )

    return columns


def __build_postgres_transit_ddl(
    transit_table: str,
    table_meta: dict[str, Any],
) -> str:
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

    return ddl


def __build_clickhouse_transit_ddl(
    transit_table: str, table_meta: dict[str, Any]
) -> str:
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
    settings = {
        "index_granularity": 8192,
        "allow_suspicious_low_cardinality_types": 1,
    }

    if table_settings := table_meta.get("settings"):
        if "index_granularity" in table_settings:
            settings.update(table_settings)

    settings_str = ", ".join(f"{k} = {v}" for k, v in settings.items())

    return (
        f"CREATE TABLE IF NOT EXISTS {transit_table} (\n"
        f"{columns_str}\n"
        f")\n"
        f"ENGINE = MergeTree{partition_clause}\n"
        f"ORDER BY ({order_by_str})\n"
        f"SETTINGS {settings_str}"
    )


def build_transit_ddl(
    transit_table: str,
    table_meta: dict[str, Any],
    is_postgres: bool,
) -> str:
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
        is_postgres = type(cursor) is Cursor
        transit_ddl = build_transit_ddl(
            transit_table,
            table_meta,
            is_postgres,
        )
        return ETLInfo(
            table_name,
            table_ddl,
            transit_table,
            transit_ddl,
            normalize_metadata(table_meta, is_postgres),
        )
    except Exception as error:
        raise errors.DBHoseAirflowError(error)
