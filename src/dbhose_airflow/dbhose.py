from base_dumper import (
    CompressionLevel,
    CompressionMethod,
    DumperMode,
    DumperType,
    DumpFormat,
    IsolationLevel,
    Timeout,
)

from .common import (
    ColumnMeta,
    DQCheck,
    Error,
    ETLInfo,
    MoveMethod,
    TableMetadata,
    define_connector,
    define_dumper,
    define_query,
    generate_ddl,
    logo,
    wrap_frame,
)


class DBHose:
    """DBHose object."""

    def __init__(
        self,
        table_dest: str,
        connection_dest: str,
        connection_src: str | None = None,
        # dq_skip_check: list[str] = [],
        # filter_by: list[str] = [],
        # drop_temp_table: bool = True,
        # move_method: MoveMethod = MoveMethod.replace,
        # custom_move: str | None = None,
        # compress_method: CompressionMethod = CompressionMethod.ZSTD,
        # timeout: int = DBMS_DEFAULT_TIMEOUT_SEC,
    ) -> None:
        """Class initialization."""
