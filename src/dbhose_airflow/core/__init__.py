"""Rust module for get DDLs from servers."""

from . import errors as Error
from .ddl import generate_ddl
from .ddl_core import (
    clickhouse_ddl,
    postgres_ddl,
    postgres_sequence_ddl,
)
from .move import (
    AppendStrategy,
    AutoStrategy,
    CustomStrategy,
    DeleteStrategy,
    ReplaceStrategy,
    RewriteStrategy,
    MoveStrategy,
    get_move_strategy,
)
from .structs import (
    ConnectionConfig,
    ColumnMeta,
    DQCheck,
    DQConfig,
    DQTest,
    ETLInfo,
    MoveMethod,
    MoveType,
    StagingConfig,
    TableMetadata,
)


__all__ = (
    "AppendStrategy",
    "AutoStrategy",
    "ConnectionConfig",
    "ColumnMeta",
    "CustomStrategy",
    "DeleteStrategy",
    "DQCheck",
    "DQConfig",
    "DQTest",
    "Error",
    "ETLInfo",
    "MoveStrategy",
    "MoveMethod",
    "MoveType",
    "ReplaceStrategy",
    "RewriteStrategy",
    "StagingConfig",
    "TableMetadata",
    "clickhouse_ddl",
    "generate_ddl",
    "get_move_strategy",
    "postgres_ddl",
    "postgres_sequence_ddl",
)
