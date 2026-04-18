from airflow.hooks.base import log
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
        filter_by: list[str] | None = None,
        drop_transit_table: bool = True,
        transit_random_postfix: bool = True,
        move_method: MoveMethod = MoveMethod.replace,
        custom_move: str | None = None,
        mode: DumperMode = DumperMode.DEBUG,
        dump_format: DumpFormat | None = None,
        timeout: int | None = None,
        exclude_transit_table: bool = False,
        dq_skip_check: list[str] | None = None,
        dq_custom_checks: str | list[str] | None = None,
        dq_exclude_columns: list[str] | None = None,
        dq_external_connection: str | None = None,
        dq_comparison_object: str | None = None,
        dq_comparsion_object_associate: dict[str, str] | None = None,
        dq_object_use_src_connection: bool = False,
        isolation_src: IsolationLevel = IsolationLevel.committed,
        isolation_dest: IsolationLevel = IsolationLevel.committed,
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

        self.logger = log
        self.table_dest = table_dest
        self.connection_dest = connection_dest
        self.connection_src = connection_src
        self.filter_by = filter_by
        self.drop_transit_table=drop_transit_table
        self.transit_random_postfix=transit_random_postfix
        self.move_method = move_method
        self.custom_move = custom_move
        self.mode = mode
        self.dump_format = dump_format
        self.timeout = timeout
        self.exclude_transit_table = exclude_transit_table
        self.dq_skip_check = dq_skip_check
        self.dq_custom_checks=dq_custom_checks
        self.dq_exclude_columns=dq_exclude_columns
        self.dq_external_connection = dq_external_connection
        self.dq_comparison_object = dq_comparison_object
        self.dq_comparsion_object_associate=dq_comparsion_object_associate
        self.dq_object_use_src_connection=dq_object_use_src_connection
        self.isolation_src = isolation_src
        self.isolation_dest = isolation_dest
        self.compress_src = compress_src
        self.compress_dest = compress_dest
        self.compress_level_src = compress_level_src
        self.compress_level_dest = compress_level_dest
        self.dumper_dest = None
        self.dumper_src = None
        self.dumper_dq = None
        self.etl_info = None
        self.dq_comparsion_metadata = None
        self.define_etl_params()

    def define_etl_params(self) -> None:
        """Define all connections and ETL objects."""

        self.logger.info(logo())
        self.dumper_dest = define_dumper(
            self.connection_dest,
            self.compress_level_dest,
            self.timeout,
            self.isolation_dest,
            self.mode,
            self.dump_format,
        )

        if self.dumper_dest.is_readonly and self.mode is not DumperMode.TEST:
            raise Error.DBHosePermissionError(
                "Read only mode. Check your permissions.",
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

        self.logger.info("Getting ETL params from source server")
        self.etl_info = generate_ddl(
            self.table_dest,
            self.dumper_dest.cursor,
            self.transit_random_postfix,
        )
        if self.dq_comparison_object:
            self.logger.info("Getting ETL params for comparison_object")
            dq_dumper = (
                self.dumper_src
                if self.dq_object_use_src_connection
                else self.dumper_dest
            )
            comparsion_dumper = self.dumper_dq or dq_dumper
            self.dq_comparsion_metadata = generate_ddl(
                self.dq_comparison_object,
                comparsion_dumper.cursor,
                without_transit=True,
            )
        self.logger.info("ETL params defined")

    def create_transit(self) -> None:
        """Create transit table."""

        self.logger.info(wrap_frame(
            f"Make table {self.etl_info.transit_table} operation started",
        ))
        self.dumper_dest.cursor.execute(self.etl_info.transit_ddl)
        self.logger.info(wrap_frame(
            f"Table {self.etl_info.transit_table} created",
        ))

    def drop_transit(self) -> None:
        """Drop transit table."""

        if self.drop_transit_table:
            self.logger.info("Drop transit table operation start")
            self.dumper_dest.cursor.execute(
                f"drop table if exists {self.etl_info.transit_table}"
            )
            self.logger.info(wrap_frame(
                f"Table {self.etl_info.transit_table} dropped",
            ))
        else:
            self.logger.warning(
                wrap_frame("Drop transit table operation skipped by user")
            )
