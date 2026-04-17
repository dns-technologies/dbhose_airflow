"""Rust module for get DDLs from servers."""

from .ddl_core import (
    clickhouse_ddl,
    postgres_ddl,
    postgres_sequence_ddl,
)


__all__ = (
    "clickhouse_ddl",
    "postgres_ddl",
    "postgres_sequence_ddl",
)
