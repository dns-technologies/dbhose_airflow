from collections.abc import Iterable
from io import BufferedReader
from typing import (
    Any,
    Callable,
)

from airflow.hooks.base import log
from base_dumper import (
    DumperMode,
    DumperType,
    DumpFormat,
    chunk_query,
)
from native_dumper import NativeDumper
from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
)

from .common import (
    ConnectionConfig,
    DQCheck,
    DQConfig,
    StagingConfig,
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
    """DBHose ETL orchestrator."""

    def __init__(
        self,
        destination_table: str,
        destination_conn: str | ConnectionConfig,
        source_conn: str | ConnectionConfig | None = None,
        dq_extra_conn: str | ConnectionConfig | None = None,
        *,
        source_filter: list[str] | None = None,
        staging: StagingConfig | None = None,
        move_method: MoveMethod = MoveMethod.replace,
        custom_move_sql: str | None = None,
        mode: DumperMode = DumperMode.DEBUG,
        dump_format: DumpFormat | None = None,
        dq: DQConfig | None = None,
    ) -> None:
        """Initialize DBHose orchestrator.

        Args:
            destination_table: Fully qualified table name
                               (e.g., "schema.table")
            destination_conn: Destination connection airflow_conn_id
                              or configuration
            source_conn: Source connection airflow_conn_id or
                         configuration (if None, destination is used)
            dq_extra_conn: DQ object external connection
                           airflow_conn_id or configuration or None
            source_filter: List of WHERE conditions for source query
            staging: Staging table configuration
            move_method: Method for moving data from staging to destination
            custom_move_sql: Custom SQL for move_method.CUSTOM
            mode: Operation mode (DEBUG, TEST, PRODUCTION)
            dump_format: Override dump format (auto-detected if None)
            dq: Data Quality check configuration
        """

        if not destination_table:
            raise Error.DBHoseNotFoundError("destination_table is requiered.")

        if not destination_conn:
            raise Error.DBHoseNotFoundError("destination_conn is requiered.")

        self.logger = log
        self.destination_table = destination_table
        self.destination_conn = self._init_conn(destination_conn)
        self.source_conn = self._init_conn(source_conn)
        self.dq_extra_conn = self._init_conn(dq_extra_conn, destination_conn)
        self.source_filter = source_filter or []
        self.staging = staging or StagingConfig()
        self.move_method = move_method
        self.custom_move_sql = custom_move_sql
        self.mode = mode
        self.dump_format = dump_format
        self.dq = dq or DQConfig()
        self.dumper_dest: DumperType | None = None
        self.dumper_src: DumperType | None = None
        self.dumper_dq: DumperType | None = None
        self.etl_info: ETLInfo | None = None
        self.target_table: str | None = None
        self.comparison_metadata: TableMetadata | None = None
        self._initialize()

    @staticmethod
    def etl_pipeline(dbhose_method: Callable) -> Callable:
        """Decorator that manages the full ETL pipeline.

        Handles:
        - Staging table creation (unless use_origin=True)
        - Data Quality checks (if configured)
        - Moving data to destination
        - Staging table cleanup (unless drop_after=False)
        """

        def wrapper(self: DBHose, *args, **kwargs) -> None:

            try:
                self.create_staging()
                self.logger.info(wrap_frame(
                    f"Loading data to {self.target_table} table"
                ))
                dbhose_method(self, *args, **kwargs)
                self.run_dq_checks()
                self.move_to_destination()
            finally:
                self.drop_staging()

        return wrapper

    @staticmethod
    def _init_conn(
        connection: str | ConnectionConfig | None,
        parent_config: ConnectionConfig | None = None,
    ) -> ConnectionConfig | None:
        """Connection initialization."""

        if not connection:
            return

        if isinstance(connection, str):
            if parent_config:
                return ConnectionConfig(
                    connection,
                    parent_config.isolation,
                    parent_config.compression,
                    parent_config.compression_level,
                    parent_config.timeout,
                )

            return ConnectionConfig(connection)

        if isinstance(connection, ConnectionConfig):
            return connection

        raise Error.DBHoseValueError(
            "connector must be airflow_conn_id or ConnectionConfig struct"
        )

    def _initialize(self) -> None:
        """Initialize connections and fetch ETL metadata."""

        self.logger.info(logo())
        self.dumper_dest = define_dumper(
            self.destination_conn.conn_id,
            self.destination_conn.compression_level,
            self.destination_conn.timeout,
            self.destination_conn.isolation,
            self.mode,
            self.dump_format,
        )
        self._check_readonly()

        if self.source_conn:
            self.dumper_src = define_dumper(
                self.source_conn.conn_id,
                self.source_conn.compression_level,
                self.source_conn.timeout,
                self.source_conn.isolation,
                self.mode,
                self.dump_format,
            )

        if not self.dump_format and self.dumper_src:
            if self.dumper_dest.__class__ is not self.dumper_src.__class__:
                self.dump_format = DumpFormat.CSV
            else:
                self.dump_format = DumpFormat.BINARY

            self.dumper_src.dump_format = self.dump_format
            self.dumper_dest.dump_format = self.dump_format
            self.logger.info(wrap_frame(
                f"Dump format mode switch to {self.dump_format.name}",
            ))

        if self.dq_extra_conn:
            self.dumper_dq = define_dumper(
                self.dq_extra_conn.conn_id,
                self.dq_extra_conn.compression_level,
                self.dq_extra_conn.timeout,
                self.dq_extra_conn.isolation,
                self.mode,
                self.dump_format,
            )
        elif self.dq.use_source_conn:
            self.dumper_dq = self.dumper_src
        else:
            self.dumper_dq = self.dumper_dest

        self.logger.info("Fetching ETL metadata from destination server")
        self.etl_info = generate_ddl(
            self.destination_table,
            self.dumper_dest.cursor,
            staging_random_suffix=self.staging.random_suffix,
        )

        if self.dq.comparison_table:
            self.logger.info("Fetching metadata for comparison table")
            self.comparison_metadata = generate_ddl(
                self.dq.comparison_table,
                self.dumper_dq.cursor,
                skip_staging=True,
            )

        self.target_table = (
            self.destination_table
            if self.staging.use_origin
            else self.etl_info.staging_table
        )
        self.logger.info("ETL initialization completed")

    def _check_readonly(self) -> None:
        """Check if dumper_dest is in read-only mode."""

        if self.dumper_dest.is_readonly and self.mode is not DumperMode.TEST:
            raise Error.DBHosePermissionError(
                "Read-only mode detected for destination connection. "
                "Check permissions.",
            )

    def _validate_move_requirements(self) -> None:
        """Validate that all requirements for the move method are met."""

        if self.move_method.need_filter and not self.source_filter:
            raise Error.DBHoseValueError(
                "You must specify columns in source_filter"
            )

        if self.move_method.is_custom and not self.custom_move_sql:
            raise Error.DBHoseValueError(
                "You must specify custom query in custom_move_sql"
            )

        if self._is_unsupported_delete():
            raise Error.DBHoseValueError(
                "Too many columns in filter_by (> 4) for ClickHouse DELETE"
            )

    def _is_unsupported_delete(self) -> bool:
        """Check if DELETE method is used with unsupported configuration."""

        return (
            self.move_method is MoveMethod.delete
            and self.dumper_dest.__class__ is NativeDumper
            and len(self.source_filter) > 4
        )

    def _execute_custom_move(self) -> None:
        """Execute custom SQL move."""

        for query in sum(chunk_query(self.custom_move_sql), []):
            self.dumper_dest.cursor.execute(query)

    def _execute_sql_move(self) -> None:
        """Execute SQL-based move (DELETE, REPLACE)."""

        move_query = self._get_move_query()

        for query in sum(chunk_query(move_query), []):
            self.dumper_dest.cursor.execute(query)


    def _get_move_query(self) -> str:
        """Fetch and validate the SQL query for the move method."""

        move_query = define_query(
            self.dumper_dest.dbname,
            self.move_method,
        )
        reader = self.dumper_dest.to_reader(move_query.format(
            table_dest=self.destination_table,
            table_temp=self.target_table,
            filter_by=self.source_filter,
        ))
        is_available, query = tuple(*reader.to_rows())

        if not is_available or not query:
            raise Error.DBHoseValueError(
                f"Method {self.move_method.name} is not available for "
                f"{self.destination_table}. Use another method."
            )

        return query


    def _execute_direct_move(self) -> None:
        """Execute direct data move (APPEND, REWRITE)."""

        if self.move_method is MoveMethod.rewrite:
            self.logger.info("Clearing destination table")
            self.dumper_dest.cursor.execute(
                f"TRUNCATE TABLE {self.destination_table}"
            )

        self.dumper_dest.write_between(
            self.target_table,
            self.destination_table,
        )

    def create_staging(self) -> None:
        """Create staging table."""

        if not self.staging.use_origin:
            self.logger.info(wrap_frame(
                f"Creating staging table {self.etl_info.staging_table}",
            ))
            self.dumper_dest.cursor.execute(self.etl_info.staging_ddl)
            self.logger.info(wrap_frame(
                f"Staging table {self.etl_info.staging_table} created",
            ))

    def drop_staging(self) -> None:
        """Drop staging table if configured."""

        if not self.staging.use_origin:
            if not self.staging.drop_after:
                return self.logger.warning(wrap_frame(
                    "Staging table drop skipped by configuration",
                ))

            self.logger.info("Dropping staging table")
            self.dumper_dest.cursor.execute(
                f"DROP TABLE IF EXISTS {self.etl_info.staging_table}",
            )
            self.logger.info(wrap_frame(
                f"Staging table {self.etl_info.staging_table} dropped",
            ))

    def move_to_destination(self) -> None:
        """Move data from staging to destination table."""

        if not self.staging.use_origin:
            self.logger.info(wrap_frame(
                f"Moving data using method: {self.move_method.name}",
            ))
            self._validate_move_requirements()

            if self.move_method.is_custom:
                self._execute_custom_move()
            elif self.move_method.have_sql:
                self._execute_sql_move()
            else:
                self._execute_direct_move()

            self.logger.info(wrap_frame(
                f"Data moved into {self.destination_table}",
            ))

    def run_dq_checks(self) -> None:
        """Run configured Data Quality checks."""

        if not self.dq:
            return self.logger.info("No DQ checks configured, skipping")

        self.logger.info(wrap_frame("Running Data Quality checks"))

        for test in DQCheck._member_names_:
            dq = DQCheck[test]

            if test in self.dq.disabled_checks:
                self.logger.warning(
                    wrap_frame(f"{dq.description} test skipped by user")
                )
                continue

            if dq.need_source_table and not self.dq.comparison_table:
                self.logger.warning(
                    wrap_frame(
                        f"{dq.description} test skipped [no comparison object]"
                    ),
                )
                continue

            query_dest = define_query(self.dumper_dest.dbname, dq)

            if dq.need_source_table:
                query_src = define_query(self.dumper_dq.dbname, dq)

                if dq.generate_queryes:
                    reader_src = self.dumper_dq.to_reader(
                        query_src.format(table=self.dq.comparison_table),
                    )
                    tests_src = list(reader_src.to_rows())
                    have_test = next(iter(tests_src))

                    if not have_test:
                        self.logger.warning(
                            wrap_frame(f"{dq.description} test Skip "
                            "[no data types for test]"),
                        )
                        continue

                    reader_dest = self.dumper_dest.to_reader(
                        query_dest.format(table=self.target_table),
                    )
                    tests_dest = list(reader_dest.to_rows())

                    for (_, column_src, test_src) in tests_src:
                        for (_, column_dest, test_dest) in tests_dest:
                            if column_src == column_dest:
                                reader_src = self.dumper_dq.to_reader(test_src)
                                reader_dest = self.dumper_dest.to_reader(
                                    test_dest,
                                )
                                value_src = next(iter(*reader_src.to_rows()))
                                value_dst = next(iter(*reader_dest.to_rows()))

                                if value_src != value_dst:
                                    err_msg = (
                                        f"Check column {column_src} test "
                                        f"Fail: value {value_src} "
                                        f"<> {value_dst}"
                                    )
                                    self.logger.error(wrap_frame(err_msg))
                                    raise ValueError(err_msg)

                                self.logger.info(
                                    wrap_frame(
                                        f"Check column \"{column_src}\" "
                                        "test Pass",
                                    ),
                                )
                                break
                        else:
                            self.logger.warning(
                                wrap_frame(
                                    f"Check column \"{column_src}\" test Skip "
                                    "[no column for test]",
                                ),
                            )
#                 else:
#                     reader_src = dumper_src.to_reader(
#                         query_src.format(table=table),
#                     )
#                     reader_dest = self.dumper_dest.to_reader(
#                         query_dest.format(table=self.table_temp),
#                     )
#                     value_src = next(iter(reader_src.to_rows()))[0]
#                     value_dst = next(iter(reader_dest.to_rows()))[0]

#                     if value_src != value_dst:
#                         err_msg = (
#                             f"{dq.description} test Fail: "
#                             f"value {value_src} <> {value_dst}"
#                         )
#                         self.logger.error(wrap_frame(err_msg))
#                         raise ValueError(err_msg)

#             else:
#                 reader_dest = self.dumper_dest.to_reader(
#                     query_dest.format(table=self.table_temp),
#                 )

#                 if dq.generate_queryes:
#                     tests = list(reader_dest.to_rows())

#                     for (have_test, column_name, query) in tests:

#                         if not have_test:
#                             self.logger.warning(
#                                 wrap_frame(f"{dq.description} test Skip "
#                                 "[no column for test]"),
#                             )
#                             break

#                         reader_dest = self.dumper_dest.to_reader(query)
#                         value, result = next(iter(reader_dest.to_rows()))

#                         if result == "Fail":
#                             err_msg = (
#                                 f"Check column {column_name} test Fail "
#                                 f"with {value} error rows"
#                             )
#                             self.logger.error(wrap_frame(err_msg))
#                             raise ValueError(err_msg)

#                         self.logger.info(
#                             wrap_frame(
#                                 f"Check column {column_name} test Pass",
#                             ),
#                         )
#                 else:
#                     value, result = next(iter(reader_dest.to_rows()))

#                     if result == "Fail":
#                         err_msg = (
#                             f"{dq.description} test Fail "
#                             f"with {value} error rows"
#                         )
#                         self.logger.error(wrap_frame(err_msg))
#                         raise ValueError(err_msg)

            self.logger.info(wrap_frame(f"{dq.description} test Pass"))

        self.logger.info(
            wrap_frame("All Data Quality tests have been completed")
        )

    @etl_pipeline
    def from_dbms(
        self,
        query: str | None = None,
        table: str | None = None,
    ) -> None:
        """Upload from DBMS."""

        self.dumper_dest.write_between(
            self.target_table,
            table,
            query,
            self.dumper_src,
        )

    @etl_pipeline
    def from_iterable(
        self,
        dtype_data: Iterable[Any],
    ) -> None:
        """Upload from python iterable object."""

        self.dumper_dest.from_rows(dtype_data, self.target_table)

    @etl_pipeline
    def from_file(
        self,
        fileobj: BufferedReader,
    ) -> None:
        """Upload from any dump file object."""

        # TODO. Add dr_herriot author 0xMihalich
        self.dumper_dest.write_dump(fileobj, self.target_table)

    @etl_pipeline
    def from_frame(
        self,
        data_frame: PdFrame | PlFrame | LfFrame,
    ) -> None:
        """Upload from DataFrame."""

        # TODO. Add dr_herriot author 0xMihalich
        if data_frame.__class__ is PdFrame:
            return self.dumper_dest.from_pandas(data_frame, self.target_table)

        if data_frame.__class__ in (PlFrame, LfFrame):
            return self.dumper_dest.from_polars(data_frame, self.target_table)

        raise Error.DBHoseTypeError(
            f"Unknown DataFrame type {data_frame.__class__}.",
        )
