from collections.abc import Iterable
from pathlib import Path
from typing import (
    Any,
    Callable,
    NoReturn,
)

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
    define_dumper,
    define_query,
    get_logger,
    logo,
    wrap_frame,
)
from .core import (
    ConnectionConfig,
    DQCheck,
    DQConfig,
    StagingConfig,
    Error,
    ETLInfo,
    MoveMethod,
    TableMetadata,
    generate_ddl,
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
        dump_format: DumpFormat = DumpFormat.BINARY,
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
            self.error_message(
                "destination_table is requiered.",
                Error.DBHoseNotFoundError,
            )

        if not destination_conn:
            self.error_message(
                "destination_conn is requiered.",
                Error.DBHoseNotFoundError,
            )

        self.logger = get_logger()
        self.destination_table = destination_table
        self.destination_conn = self._init_conn(destination_conn)
        self.source_conn = self._init_conn(source_conn)
        self.dq_extra_conn = self._init_conn(dq_extra_conn, destination_conn)
        self.source_filter = source_filter or []
        self.staging = staging or StagingConfig()
        self.move_method = move_method
        self.custom_move_sql = custom_move_sql
        self._mode = mode
        self._dump_format = dump_format
        self.dq = dq or DQConfig()
        self.dumper_dest: DumperType | None = None
        self.dumper_src: DumperType | None = None
        self.dumper_dq: DumperType | None = None
        self.etl_info: ETLInfo | None = None
        self.target_table: str | None = None
        self.comparison_metadata: TableMetadata | None = None
        self._initialize()

    @property
    def mode(self) -> DumperMode:
        """Property method for get dumper mode."""

        return self._mode

    @mode.setter
    def mode(self, mode_value: DumperMode) -> DumperMode:
        """Property method for set dumper mode value."""

        if self._mode is not mode_value:
            self._mode = mode_value

            for dumper in (self.dumper_src, self.dumper_dest, self.dumper_dq):
                if dumper:
                    dumper.mode = self._mode

            self.logger.info(wrap_frame(
                f"DBHose mode switch to {self._mode.name}",
            ))

        return self._mode

    @property
    def dump_format(self) -> DumpFormat:
        """Property method for get dump format."""

        return self._dump_format

    @dump_format.setter
    def dump_format(self, dump_format_value: DumpFormat) -> DumpFormat:
        """Property method for set dump format value."""

        if self._dump_format is not dump_format_value:
            self._dump_format = dump_format_value

            for dumper in (self.dumper_src, self.dumper_dest, self.dumper_dq):
                if dumper:
                    dumper.dump_format = self._dump_format

            self.logger.info(wrap_frame(
                f"DBHose dump format switch to {self._dump_format.name}",
            ))

        return self._dump_format

    @staticmethod
    def etl_pipeline(dbhose_method: Callable) -> Callable:
        """Decorator that manages the full ETL pipeline.

        Handles:
        - Staging table creation (unless use_origin=True)
        - Data Quality checks (if configured)
        - Moving data to destination
        - Staging table cleanup (unless drop_after=False)
        """

        def wrapper(self: "DBHose", *args, **kwargs) -> None:

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

    def error_message(
        self,
        error: str | BaseException,
        exception: Exception = Error.DBHoseError,
    ) -> NoReturn:
        """Raise DBHose errors."""

        self.logger.error(f"{exception.__class__.__name__}: {error}")
        raise exception(error)

    def _init_conn(
        self,
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
                    parent_config.compression,
                    parent_config.compression_level,
                    parent_config.timeout,
                    parent_config.isolation,
                )

            return ConnectionConfig(connection)

        if isinstance(connection, ConnectionConfig):
            return connection

        self.error_message(
            "connector must be airflow_conn_id or ConnectionConfig struct",
            Error.DBHoseValueError,
        )

    def _initialize(self) -> None:
        """Initialize connections and fetch ETL metadata."""

        self.logger.info(logo())
        self.dumper_dest = define_dumper(
            self.destination_conn.conn_id,
            self.destination_conn.compression,
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
                self.source_conn.compression,
                self.source_conn.compression_level,
                self.source_conn.timeout,
                self.source_conn.isolation,
                self.mode,
                self.dump_format,
            )

        if not self.dump_format and self.dumper_src:
            if self.dumper_dest.__class__ is not self.dumper_src.__class__:
                self.dump_format = DumpFormat.CSV

        if self.dq_extra_conn:
            self.dumper_dq = define_dumper(
                self.dq_extra_conn.conn_id,
                self.dq_extra_conn.compression,
                self.dq_extra_conn.compression_level,
                self.dq_extra_conn.timeout,
                self.dq_extra_conn.isolation,
                self.mode,
                self.dump_format,
            )
        elif self.dq.use_destination_conn:
            self.dumper_dq = self.dumper_dest
        else:
            self.dumper_dq = self.dumper_src

        self.logger.info("Fetching ETL metadata from destination server")
        self.etl_info = generate_ddl(
            self.destination_table,
            self.dumper_dest.cursor,
            staging_random_suffix=self.staging.random_suffix,
        )

        if self.dq.comparison_object:
            self.logger.info("Fetching metadata for comparison table")
            self.comparison_metadata = generate_ddl(
                self.dq.comparison_object,
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
            self.error_message(
                "Read-only mode detected for destination connection. "
                "Check permissions.",
                Error.DBHosePermissionError,
            )

    def _validate_move_requirements(self) -> None:
        """Validate that all requirements for the move method are met."""

        if self.move_method.need_filter and not self.source_filter:
            self.error_message(
                "You must specify columns in source_filter",
                Error.DBHoseValueError,
            )

        if self.move_method.is_custom and not self.custom_move_sql:
            self.error_message(
                "You must specify custom query in custom_move_sql",
                Error.DBHoseValueError,
            )

        if self._is_unsupported_delete():
            self.error_message(
                "Too many columns in filter_by (> 4) for ClickHouse DELETE",
                Error.DBHoseValueError,
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
            self.error_message(
                f"Method {self.move_method.name} is not available for "
                f"{self.destination_table}. Use another method.",
                Error.DBHoseValueError,
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
            self.destination_table,
            self.target_table,
        )

    def _run_single_check(self, dq_check: DQCheck) -> None:
        """Run a single DQ check."""

        if self._should_skip_check(dq_check):
            return

        try:
            if dq_check.need_source_table:
                self._run_comparison_check(dq_check)
            else:
                self._run_standalone_check(dq_check)

            self.logger.info(wrap_frame(f"{dq_check.description} test Pass"))
        except Error.DBHoseValueError as error:
            self.error_message(
                f"{dq_check.description} test Fail: {error}",
                Error.DBHoseValueError,
            )
        except Exception as error:
            self.logger.error(wrap_frame(
                f"{dq_check.description} test Fail: {error}",
            ))
            self.error_message(error)

    def _should_skip_check(self, dq_check: DQCheck) -> bool:
        """Check if DQ test should be skipped."""

        if dq_check in self.dq.disabled_checks:
            self.logger.warning(wrap_frame(
                f"{dq_check.description} test skipped by user",
            ))
            return True

        if dq_check.need_source_table and not self.dq.comparison_object:
            self.logger.warning(wrap_frame(
                f"{dq_check.description} test skipped [no comparison object]",
            ))
            return True

        return False

    def _run_comparison_check(self, dq_check: DQCheck) -> None:
        """Run check comparing source and destination tables."""

        if dq_check.generate_queryes:
            self._run_comparison_with_generated_queries(dq_check)
        else:
            self._run_comparison_single_query(dq_check)

    def _run_standalone_check(self, dq_check: DQCheck) -> None:
        """Run check on destination table only."""

        if dq_check.generate_queryes:
            self._run_standalone_with_generated_queries(dq_check)
        else:
            self._run_standalone_single_query(dq_check)

    def _run_comparison_with_generated_queries(
        self,
        dq_check: DQCheck,
    ) -> None:
        """Run comparison check with dynamically generated queries."""

        query_src = define_query(self.dumper_dq.dbname, dq_check)
        query_dest = define_query(self.dumper_dest.dbname, dq_check)
        tests_src = self._fetch_tests(
            self.dumper_dq,
            query_src,
            self.dq.comparison_object,
        )

        if not self._has_tests(tests_src):
            return self.logger.warning(wrap_frame(
                f"{dq_check.description} test Skip [no data types for test]",
            ))

        tests_dest = self._fetch_tests(
            self.dumper_dest,
            query_dest,
            self.target_table,
        )

        for (_, column_src, test_src) in tests_src:
            column_dest = self.dq.column_mapping.get(column_src, column_src)

            if column_dest in self.dq.exclude_columns:
                self.logger.warning(wrap_frame(
                    f"Check column \"{column_dest}\" test skipped by user",
                ))
                continue

            matching_test = self._find_matching_test(tests_dest, column_dest)

            if matching_test:
                self._compare_values(
                    self.dumper_dq,
                    test_src,
                    self.dumper_dest,
                    matching_test,
                    column_src,
                )
            else:
                self.logger.warning(wrap_frame(
                    f"Check column \"{column_src}\" test "
                    "Skip [no column for test]",
                ))

    def _run_comparison_single_query(self, dq_check: DQCheck) -> None:
        """Run comparison check with a single query."""

        query_src = define_query(self.dumper_dq.dbname, dq_check).format(
            table=self.dq.comparison_object,
        )
        query_dest = define_query(self.dumper_dest.dbname, dq_check).format(
            table=self.target_table,
        )
        value_src = self._fetch_dq_values(self.dumper_dq, query_src)[0]
        value_dest = self._fetch_dq_values(self.dumper_dest, query_dest)[0]

        if value_src != value_dest:
            self.error_message(
                f"{dq_check.description} test Fail: "
                f"value {value_src} <> {value_dest}",
                Error.DBHoseValueError,
            )

    def _run_standalone_with_generated_queries(
        self,
        dq_check: DQCheck,
    ) -> None:
        """Run standalone check with dynamically generated queries."""

        query_dest = define_query(self.dumper_dest.dbname, dq_check)
        tests = self._fetch_tests(
            self.dumper_dest,
            query_dest,
            self.target_table,
        )

        for (have_test, column_name, query) in tests:
            if not have_test:
                self.logger.warning(wrap_frame(
                    f"{dq_check.description} test Skip [no column for test]",
                ))
                break

            self._check_single_column(column_name, query)

    def _run_standalone_single_query(self, dq_check: DQCheck) -> None:
        """Run standalone check with a single query."""

        query_dest = define_query(self.dumper_dest.dbname, dq_check).format(
            table=self.target_table,
        )
        value, result = self._fetch_dq_values(self.dumper_dest, query_dest)

        if result == "Fail":
            self.error_message(
                f"{dq_check.description} test Fail with {value} error rows",
                Error.DBHoseValueError,
            )

    def _fetch_tests(self, dumper: DumperType, query: str, table: str) -> list:
        """Fetch generated test queries."""

        reader = dumper.to_reader(query.format(table=table))
        return list(reader.to_rows())

    def _has_tests(self, tests: list) -> bool:
        """Check if any tests were generated."""

        return bool(tests) and bool(next(iter(tests)))

    def _find_matching_test(self, tests: list, column_name: str) -> str | None:
        """Find test query for a specific column."""

        for (_, col, test) in tests:
            if col == column_name:
                return test

    def _compare_values(
        self,
        dumper_src: DumperType,
        query_src: str,
        dumper_dest: DumperType,
        query_dest: str,
        column_name: str,
    ) -> None:
        """Compare values from source and destination."""

        value_src = self._fetch_dq_values(dumper_src, query_src)[0]
        value_dest = self._fetch_dq_values(dumper_dest, query_dest)[0]

        if value_src != value_dest:
            self.error_message(
                f"Check column \"{column_name}\" test Fail: "
                f"value {value_src} <> {value_dest}",
                Error.DBHoseValueError,
            )

        self.logger.info(wrap_frame(f'Check column "{column_name}" test Pass'))

    def _check_single_column(self, column_name: str, query: str) -> None:
        """Check a single column with generated query."""

        reader = self.dumper_dest.to_reader(query)
        value, result = next(iter(reader.to_rows()))

        if result == "Fail":
            self.error_message(
                f"Check column \"{column_name}\" test "
                f"Fail with {value} error rows",
                Error.DBHoseValueError,
            )

        self.logger.info(wrap_frame(f'Check column "{column_name}" test Pass'))

    def _fetch_dq_values(self, dumper: DumperType, query: str) -> tuple:
        """Fetch a single value from query result."""

        reader = dumper.to_reader(query)
        return next(iter(reader.to_rows()))

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
            self.dump_format = DumpFormat.BINARY
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

        self.logger.info(wrap_frame("Running Data Quality checks"))
        self.dump_format = DumpFormat.BINARY

        for check_name in DQCheck._member_names_:
            dq_check = DQCheck[check_name]
            self._run_single_check(dq_check)

        self.logger.info(wrap_frame(
            "All Data Quality tests have been completed"
        ))

    @etl_pipeline
    def from_dbms(
        self,
        query: str | None = None,
        table: str | None = None,
    ) -> None:
        """Upload from DBMS."""

        self.dumper_dest.write_between(
            table_dest=self.target_table,
            table_src=table,
            query_src=query,
            dumper_src=self.dumper_src,
        )

    @etl_pipeline
    def from_iterable(
        self,
        dtype_data: Iterable[Any],
    ) -> None:
        """Upload from python iterable object."""

        self.dump_format = DumpFormat.BINARY
        dtype_data, source = self.dumper_dest._db_meta_from_iter(dtype_data)
        self.dumper_dest.from_rows(dtype_data, self.target_table, source)

    @etl_pipeline
    def from_file(
        self,
        file_path: str | Path,
    ) -> None:
        """Upload from any dump file."""

        # TODO. Add dr_herriot author 0xMihalich
        try:
            with open(file_path, "rb") as fileobj:
                self.dumper_dest.write_dump(fileobj, self.target_table)
        except PermissionError as error:
            self.error_message(error, Error.DBHosePermissionError)
        except FileNotFoundError as error:
            self.error_message(error, Error.DBHoseNotFoundError)
        except Exception as error:
            self.error_message(error)

    @etl_pipeline
    def from_frame(
        self,
        data_frame: PdFrame | PlFrame | LfFrame,
    ) -> None:
        """Upload from DataFrame."""

        self.dump_format = DumpFormat.BINARY

        if data_frame.__class__ is PdFrame:
            return self.dumper_dest.from_pandas(data_frame, self.target_table)

        if data_frame.__class__ in (PlFrame, LfFrame):
            return self.dumper_dest.from_polars(data_frame, self.target_table)

        self.error_message(
            f"Unknown DataFrame type {data_frame.__class__}.",
            Error.DBHoseTypeError,
        )
