from airflow.hooks.base import log as logger
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
        dq_skip_check: list[str] | None = None,
        filter_by: list[str] | None = None,
        drop_temp_table: bool = True,
        move_method: MoveMethod = MoveMethod.replace,
        custom_move: str | None = None,
        mode: DumperMode = DumperMode.DEBUG,
        dump_format: DumpFormat | None = None,
        timeout: int | None = None,
        isolation_src: IsolationLevel = IsolationLevel.committed,
        isolation_dest: IsolationLevel = IsolationLevel.committed,
        dq_comparison_object: str | None = None,
        dq_external_connection: str | None = None,
        compress_src: CompressionMethod = CompressionMethod.ZSTD,
        compress_dest: CompressionMethod = CompressionMethod.ZSTD,
        compress_level_src: int = CompressionLevel.ZSTD_DEFAULT,
        compress_level_dest: int = CompressionLevel.ZSTD_DEFAULT,
    ) -> None:
        """Class initialization."""

        if not table_dest:
            raise Error.DBHoseValueError("Table not defined.")

        if not connection_dest:
            raise Error.DBHoseValueError("Source connection not defined.")

        self.logger = logger
        self.table_dest = table_dest
        self.connection_dest = connection_dest
        self.connection_src = connection_src
        self.dq_skip_check = dq_skip_check
        self.filter_by = filter_by
        self.drop_temp_table = drop_temp_table
        self.move_method = move_method
        self.custom_move = custom_move
        self.mode = mode
        self.dump_format = dump_format
        self.timeout = timeout
        self.isolation_src = isolation_src
        self.isolation_dest = isolation_dest
        self.dq_comparison_object = dq_comparison_object
        self.dq_external_connection = dq_external_connection
        self.compress_src = compress_src
        self.compress_dest = compress_dest
        self.compress_level_src = compress_level_src
        self.compress_level_dest = compress_level_dest

        self.logger.info(logo())

        self.dumper_src = None
        self.dumper_dq = None
        self.dumper_dest = define_dumper(
            self.connection_dest,
            self.compress_level_dest,
            self.timeout,
            self.isolation_dest,
            self.mode,
            self.dump_format,
        )

        if self.connection_src:
            self.dumper_src = define_dumper(
                self.connection_src,
                self.compress_level_src,
                self.timeout,
                self.isolation_src,
                self.mode,
                self.dump_format,
            )

        if self.dq_external_connection:
            self.dumper_dq = define_dumper(
                self.connection_dest,
                self.compress_level_dest,
                self.timeout,
            )
