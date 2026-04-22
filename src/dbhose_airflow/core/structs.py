from dataclasses import (
    dataclass,
    field,
)
from enum import Enum
from typing import (
    Any,
    NamedTuple,
)

from base_dumper import (
    CompressionLevel,
    CompressionMethod,
    IsolationLevel,
)


class DQTest(NamedTuple):
    """Data quality test."""

    description: str
    generate_queryes: int
    need_source_table: int


class DQCheck(DQTest, Enum):
    """Enum for avaliable tests."""

    empty = DQTest("Table not empty", 0, 0)
    uniq = DQTest("Table don't have any duplicate rows", 0, 0)
    future = DQTest("Table don't have dates from future", 1, 0)
    infinity = DQTest("Table don't have infinity values", 1, 0)
    nan = DQTest("Table don't have NaN values", 1, 0)
    total = DQTest("Equal data total rows count between objects", 0, 1)
    sum = DQTest("Equal data sums in digits columns between objects", 1, 1)


class MoveType(NamedTuple):
    """Move method metadata."""

    description: str
    requires_partition: bool = False
    requires_filter: bool = False
    use_temp_table: bool = False


class MoveMethod(MoveType, Enum):
    """Data movement methods from staging to destination."""

    APPEND = MoveType(
        "Simple INSERT - adds new rows without deleting old ones",
        use_temp_table=True,
    )
    REWRITE = MoveType(
        "TRUNCATE + INSERT - completely replaces table content",
        use_temp_table=True,
    )
    DELETE = MoveType(
        "DELETE matching rows + INSERT - for incremental updates",
        requires_filter=True,
    )
    REPLACE = MoveType(
        "REPLACE/ATTACH PARTITION - atomic partition replacement",
        requires_partition=True,
    )
    AUTO = MoveType(
        "Automatically selects best strategy based on table metadata",
    )
    CUSTOM = MoveType(
        "User-provided custom SQL for data movement",
    )


class ColumnMeta(NamedTuple):
    """Column structure."""

    name: str
    data_type: str
    nullable: bool
    has_default: bool
    default_value: str | None
    comment: str | None
    position: int
    type_oid: int | None
    type_namespace: int | None
    generated: str | None
    identity: str | None


class TableMetadata(NamedTuple):
    """Table metadata structure."""

    name: str
    schema: str
    owner: str | None
    comment: str | None
    columns: list[ColumnMeta]
    partition_by: str | None
    order_by: list[str] | None
    primary_key: list[str] | None
    engine: str
    settings: dict[str, Any] | None
    oid: int | None


@dataclass
class ETLInfo:
    """Defines structure for ETL operations."""

    name: str
    ddl: str
    staging_table: str
    staging_temp: str
    staging_ddl: str
    staging_ddl_simple: str
    staging_ddl_temp: str
    table_metadata: TableMetadata


@dataclass
class ConnectionConfig:
    """Configuration for a single database connection."""

    conn_id: str
    compression: CompressionMethod = CompressionMethod.ZSTD
    compression_level: int = CompressionLevel.ZSTD_DEFAULT
    timeout: int | None = None
    isolation: IsolationLevel = IsolationLevel.committed


@dataclass
class StagingConfig:
    """Configuration for staging table."""

    use_origin: bool = False
    drop_after: bool = True
    random_suffix: bool = True


@dataclass
class DQConfig:
    """Configuration for Data Quality checks."""

    disabled_checks: list[DQCheck] = field(default_factory=list)
    custom_queries: list[str] = field(default_factory=list)
    exclude_columns: list[str] = field(default_factory=list)
    filter_columns: list[str] = field(default_factory=list)
    column_mapping: dict[str, str] = field(default_factory=dict)
    comparison_object: str | None = None
    use_destination_conn: bool = False
