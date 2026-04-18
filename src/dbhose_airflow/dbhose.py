from airflow.hooks.base import log
from base_dumper import (
    DumperMode,
    DumperType,
    DumpFormat,
)

from .common import (
    ConnectionConfig,
    DQConfig,
    StagingConfig,
    Error,
    ETLInfo,
    MoveMethod,
    TableMetadata,
    define_dumper,
    generate_ddl,
    logo,
    wrap_frame,
)


def __init_conn(
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


class DBHose:
    """DBHose ETL orchestrator."""

    def __init__(
        self,
        destination_table: str,
        destination_conn: str | ConnectionConfig,
        source_conn: str | ConnectionConfig | None = None,
        dq_object_conn: str | ConnectionConfig | None = None,
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
            dq_object_conn: DQ object external connection
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
        self.destination_conn = __init_conn(destination_conn)
        self.source_conn = __init_conn(source_conn)
        self.dq_object_conn = __init_conn(dq_object_conn, destination_conn)
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

        if self.dq_object_conn:
            self.dumper_dq = define_dumper(
                self.dq_object_conn.conn_id,
                self.dq_object_conn.compression_level,
                self.dq_object_conn.timeout,
                self.dq_object_conn.isolation,
                self.mode,
                self.dump_format,
            )

        self.logger.info("Fetching ETL metadata from destination server")
        self.etl_info = generate_ddl(
            self.destination_table,
            self.dumper_dest.cursor,
            staging_random_suffix=self.staging.random_suffix,
        )

        if self.dq.comparison_table:
            self.logger.info("Fetching metadata for comparison table")
            dq_dumper = (
                self.dumper_src
                if self.dq.use_source_conn and self.dumper_src
                else self.dumper_dest
            )
            comparsion_dumper = self.dumper_dq or dq_dumper
            self.comparison_metadata = generate_ddl(
                self.dq.comparison_table,
                comparsion_dumper.cursor,
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
            # ... логика перемещения ...

    def run_dq_checks(self) -> None:
        """Run configured Data Quality checks."""

        if not self.dq:
            return self.logger.info("No DQ checks configured, skipping")

        self.logger.info(wrap_frame("Running Data Quality checks"))
        # ... логика DQ проверок ...

    def from_dbms(
        self,
        query: str | None = None,
        table: str | None = None,
    ) -> None:
        """Upload from DMBS."""

        try:
            self.create_staging()
            self.logger.info(wrap_frame(
                f"Loading data to {self.target_table} table"
            ))
            self.dumper_dest.write_between(
                self.target_table,
                table,
                query,
                self.dumper_src,
            )
            self.run_dq_checks()
            self.move_to_destination()
        finally:
            self.drop_staging()
