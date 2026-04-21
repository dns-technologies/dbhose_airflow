from secrets import token_bytes
from re import compile
from typing import Any

from native_dumper import HTTPCursor
from nativelib import Column
from psycopg import Cursor

from dbhose_airflow.core.errors import (
    DBHoseError,
    DBHoseNotFoundError,
    DBHoseTypeError,
)
from dbhose_airflow.core.ddl_core import (
    clickhouse_ddl,
    postgres_ddl,
)
from dbhose_airflow.core.structs import (
    ColumnMeta,
    ETLInfo,
    TableMetadata,
)


cdef object PRIMARY_KEY = compile(r"PRIMARY KEY\s*\(([^)]+)\)")
cdef dict SERVER_NAME = {
    HTTPCursor: clickhouse_ddl,
    Cursor: postgres_ddl,
}


cdef object _normalize_postgres_meta(dict meta):
    """Normalize PostgreSQL metadata."""

    cdef dict comment, constraint, col
    cdef object table_comment = None
    cdef str pk_def, engine, access_method
    cdef object match, primary_key = None
    cdef list comments = meta.get("comments", [])
    cdef list columns_meta = meta.get("columns", [])
    cdef list columns = []
    cdef int num, attnum

    for comment in comments:
        if comment.get("objsubid") == 0:
            table_comment = comment.get("description")
            break

    for constraint in meta.get("constraints", []):
        if constraint.get("contype") == "p":
            pk_def = constraint.get("condef", "")
            match = PRIMARY_KEY.search(pk_def)
            if match:
                primary_key = []
                for c in match.group(1).split(","):
                    primary_key.append(c.strip(' \t\n\r"'))
            break

    engine = meta.get("relkind", "TABLE")
    access_method = meta.get("access_method")

    if access_method and access_method != "heap":
        engine = f"{engine}:{access_method}"

    for num in range(len(columns_meta)):
        col = columns_meta[num]
        attnum = col.get("attnum", num + 1)
        columns.append(ColumnMeta(
            name=col.get("attname"),
            data_type=col.get("typname"),
            nullable=not col.get("attnotnull", False),
            has_default=col.get("atthasdef", False),
            default_value=col.get("defaultval"),
            comment=_find_column_comment(comments, attnum),
            position=attnum,
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


cdef object _normalize_clickhouse_meta(dict meta):
    """Normalize ClickHouse metadata."""

    cdef str engine = meta.get("engine") or "View"
    cdef list columns_meta = meta.get("columns", [])
    cdef list columns = []
    cdef int num
    cdef dict col
    cdef object column
    cdef bint is_generated
    cdef object generated

    for num in range(len(columns_meta)):
        col = columns_meta[num]
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


cdef object _find_column_comment(list comments, int attnum):
    """Find comment for specific column by attnum."""

    cdef int i
    cdef dict comment
    cdef int objsubid

    for i in range(len(comments)):
        comment = comments[i]
        objsubid = comment.get("objsubid", 0)
        if objsubid == attnum:
            return comment.get("description")

    return None


cdef object _pg_generated(str value):
    """Convert PostgreSQL attgenerated to unified format."""

    if value == "s":
        return "STORED"

    if value == "v":
        return "VIRTUAL"

    return None


cdef object _pg_identity(str value):
    """Convert PostgreSQL attidentity to unified format."""

    if value == "a":
        return "ALWAYS"

    if value == "d":
        return "BY DEFAULT"

    return None


cdef object _parse_reloptions(list options):
    """Parse PostgreSQL reloptions array to dict."""

    cdef int i
    cdef str opt
    cdef int eq_pos
    cdef str k, v
    cdef dict result = {}

    if not options:
        return None

    for i in range(len(options)):
        opt = options[i]
        eq_pos = opt.find("=")
        if eq_pos != -1:
            k = opt[:eq_pos].strip(" \t\n\r")
            v = opt[eq_pos + 1:].strip(" \t\n\r")
            result[k] = v

    return result if result else None


cdef object normalize_metadata(dict table_meta, bint is_postgres):
    """Convert PostgreSQL or ClickHouse metadata to unified format."""

    if is_postgres:
        return _normalize_postgres_meta(table_meta)

    return _normalize_clickhouse_meta(table_meta)


cdef list __validate_ddl(dict table_meta):
    """Validate DDL data."""

    cdef list columns = table_meta.get("columns", [])

    if not columns:
        raise DBHoseNotFoundError(
            "No columns found in table metadata",
        )

    return columns


cdef str __build_postgres_staging_ddl_simple(
    str staging_table,
    dict table_meta,
):
    """Build simple UNLOGGED staging table DDL for PostgreSQL."""

    cdef list columns = __validate_ddl(table_meta)
    cdef list col_defs = []
    cdef int i
    cdef dict col
    cdef str col_name, col_type, col_def
    cdef bint not_null
    cdef str columns_str
    cdef str ddl
    cdef list distkey
    cdef str partition_key

    for i in range(len(columns)):
        col = columns[i]
        col_name = f'"{col["attname"]}"'
        col_type = col["typname"]
        not_null = col.get("attnotnull", False)

        if not_null:
            col_def = f"    {col_name} {col_type} NOT NULL"
        else:
            col_def = f"    {col_name} {col_type}"

        col_defs.append(col_def)

    columns_str = ",\n".join(col_defs)
    ddl = (
        f"CREATE UNLOGGED TABLE IF NOT EXISTS {staging_table} (\n"
        f"{columns_str}\n) WITH (autovacuum_enabled = false)"
    )

    distkey = table_meta.get("distkey")

    if distkey:
        quoted_cols = [f'"{c}"' for c in distkey]
        ddl += f"\nDISTRIBUTED BY ({', '.join(quoted_cols)})"

    partition_key = table_meta.get("partition_key")

    if partition_key:
        ddl += f"\nPARTITION BY {partition_key}"

    return ddl


cdef str __build_postgres_staging_ddl_full(
    str staging_table,
    dict table_meta,
):
    """Build full staging table DDL for PostgreSQL using LIKE."""

    cdef str source_table = (
        f"{table_meta['schema_name']}.{table_meta['relname']}"
    )
    return (
        f"CREATE UNLOGGED TABLE IF NOT EXISTS {staging_table} "
        f"(LIKE {source_table} INCLUDING ALL)"
    )


cdef str __build_clickhouse_staging_ddl_simple(
    str staging_table,
    dict table_meta,
):
    """Build simple staging table DDL for ClickHouse with Log engine."""

    cdef list columns = __validate_ddl(table_meta)
    cdef list col_defs = []
    cdef int i
    cdef dict col
    cdef str col_name, col_type

    for i in range(len(columns)):
        col = columns[i]
        col_name = f"`{col['name']}`"
        col_type = col["data_type"]
        col_defs.append(f"    {col_name} {col_type}")

    columns_str = ",\n".join(col_defs)

    return (
        f"CREATE TABLE IF NOT EXISTS {staging_table} (\n"
        f"{columns_str}\n)\nENGINE = Log\n"
        "SETTINGS allow_suspicious_low_cardinality_types = 1"
    )


cdef str __build_clickhouse_staging_ddl_full(
    str staging_table,
    dict table_meta,
):
    """Build full staging table DDL for ClickHouse with MergeTree engine."""

    cdef list columns = __validate_ddl(table_meta)
    cdef list col_defs = []
    cdef int i
    cdef dict col
    cdef str col_name, col_type
    cdef str columns_str
    cdef list order_by
    cdef str order_by_str
    cdef str partition_by
    cdef str partition_clause
    cdef dict settings
    cdef dict table_settings
    cdef str settings_str
    cdef list order_parts = []
    cdef str part

    for i in range(len(columns)):
        col = columns[i]
        col_name = f"`{col['name']}`"
        col_type = col["data_type"]
        col_defs.append(f"    {col_name} {col_type}")

    columns_str = ",\n".join(col_defs)
    order_by = table_meta.get("order_by")

    if order_by:
        for part in order_by:
            if "(" in part:
                order_parts.append(part)
            else:
                order_parts.append(f"`{part}`")

        order_by_str = ", ".join(order_parts)

        if len(order_parts) > 1:
            order_by_str = f"({order_by_str})"
    else:
        order_by_str = f"`{columns[0]['name']}`"

    partition_by = table_meta.get("partition_by")
    partition_clause = f"\nPARTITION BY {partition_by}" if partition_by else ""
    settings = {
        "index_granularity": 8192,
        "allow_suspicious_low_cardinality_types": 1,
    }
    table_settings = table_meta.get("settings")

    if table_settings and "index_granularity" in table_settings:
        settings.update(table_settings)

    settings_str = ", ".join(f"{k} = {v}" for k, v in settings.items())

    return (
        f"CREATE TABLE IF NOT EXISTS {staging_table} (\n"
        f"{columns_str}\n)\n"
        f"ENGINE = MergeTree{partition_clause}\n"
        f"ORDER BY {order_by_str}\n"
        f"SETTINGS {settings_str}"
    )


cdef tuple build_staging_ddls(
    str staging_table,
    dict table_meta,
    bint is_postgres,
):
    """Generate both DDLs for staging table."""

    if is_postgres:
        return (
            __build_postgres_staging_ddl_full(staging_table, table_meta),
            __build_postgres_staging_ddl_simple(staging_table, table_meta),
        )

    return (
        __build_clickhouse_staging_ddl_full(staging_table, table_meta),
        __build_clickhouse_staging_ddl_simple(staging_table, table_meta),
    )


def generate_ddl(
    table_name: str,
    cursor: Cursor | HTTPCursor,
    staging_random_suffix: bool = True,
    skip_staging: bool = False,
) -> ETLInfo | TableMetadata:
    """Generate DDL and staging table."""

    ddl_core = SERVER_NAME.get(cursor.__class__)

    if not ddl_core:
        raise DBHoseTypeError("No DDL method found.")

    try:
        table_ddl, table_meta = ddl_core(cursor, table_name)
        is_postgres = type(cursor) is Cursor
        table_metadata = normalize_metadata(table_meta, is_postgres)

        if skip_staging:
            return table_metadata

        if table_name[-1] in ["`", '"']:
            _table_name = table_name[:-1]
            _closing = table_name[-1]
        else:
            _table_name = table_name
            _closing = ""

        staging_table = f"{_table_name}_staging"

        if staging_random_suffix:
            staging_table += token_bytes(4).hex()

        staging_table += _closing
        staging_ddl_full, staging_ddl_simple = build_staging_ddls(
            staging_table,
            table_meta,
            is_postgres,
        )

        return ETLInfo(
            name=table_name,
            ddl=table_ddl,
            staging_table=staging_table,
            staging_ddl=staging_ddl_full,
            staging_ddl_simple=staging_ddl_simple,
            table_metadata=table_metadata,
        )
    except Exception as error:
        raise DBHoseError(error)
