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


class DBHose:
    """DBHose ETL orchestrator."""

    def __init__(
        self,
        destination_table: str,
        destination_conn: ConnectionConfig,
        *,
        source_conn: ConnectionConfig | None = None,
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
            destination_conn: Destination connection configuration
            source_conn: Source connection configuration
                         (if None, destination is used)
            source_filter: List of WHERE conditions for source query
            staging: Staging table configuration
            move_method: Method for moving data from staging to destination
            custom_move_sql: Custom SQL for move_method.CUSTOM
            mode: Operation mode (DEBUG, TEST, PRODUCTION)
            dump_format: Override dump format (auto-detected if None)
            dq: Data Quality check configuration
        """

        self._validate_inputs(destination_table, destination_conn, source_conn)

        self.logger = log

        # Core settings
        self.destination_table = destination_table
        self.destination_conn = destination_conn
        self.source_conn = source_conn
        self.source_filter = source_filter or []

        # Staging settings
        self.staging = staging or StagingConfig()

        # Move settings
        self.move_method = move_method
        self.custom_move_sql = custom_move_sql

        # Dumper settings
        self.mode = mode
        self.dump_format = dump_format

        # DQ settings
        self.dq = dq or DQConfig()

        # Runtime state
        self.dumper_dest: DumperType | None = None
        self.dumper_src: DumperType | None = None
        self.dumper_dq: DumperType | None = None
        self.etl_info: ETLInfo | None = None
        self.comparison_metadata: TableMetadata | None = None

        self._initialize()

    def _validate_inputs(
        self,
        destination_table: str,
        destination_conn: ConnectionConfig,
        source_conn: ConnectionConfig | None,
    ) -> None:
        """Validate required inputs."""

        if not destination_table:
            raise Error.DBHoseValueError("destination_table is required")

        if not destination_conn or not destination_conn.conn_id:
            raise Error.DBHoseValueError(
                "destination_conn.conn_id is required"
            )

        if source_conn and not source_conn.conn_id:
            raise Error.DBHoseValueError("source_conn.conn_id is required")

    def _initialize(self) -> None:
        """Initialize connections and fetch ETL metadata."""

        self.logger.info(logo())

        # Initialize destination dumper
        self.dumper_dest = define_dumper(
            self.destination_conn.conn_id,
            self.destination_conn.compression_level,
            self.destination_conn.timeout,
            self.destination_conn.isolation,
            self.mode,
            self.dump_format,
        )

        self._check_readonly(self.dumper_dest, "destination")

        # Initialize source dumper if provided
        if self.source_conn:
            self.dumper_src = define_dumper(
                self.source_conn.conn_id,
                self.source_conn.compression_level,
                self.source_conn.timeout,
                self.source_conn.isolation,
                self.mode,
                self.dump_format,
            )

        # Initialize DQ external connection if provided
        if self.dq.external_conn_id:
            self.dumper_dq = define_dumper(
                self.dq.external_conn_id,
                self.destination_conn.compression_level,
                self.destination_conn.timeout,
            )

        # Fetch ETL metadata
        self.logger.info("Fetching ETL metadata from destination server")
        self.etl_info = generate_ddl(
            self.destination_table,
            self.dumper_dest.cursor,
            staging_random_suffix=self.staging.random_suffix,
        )

        # Fetch comparison table metadata if needed
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

        self.logger.info("ETL initialization completed")

    def _check_readonly(self, dumper: DumperType, name: str) -> None:
        """Check if dumper is in read-only mode."""

        if dumper.is_readonly and self.mode is not DumperMode.TEST:
            raise Error.DBHosePermissionError(
                f"Read-only mode detected for {name} connection. "
                "Check permissions.",
            )

    # Staging Table Operations

    def create_staging(self) -> None:
        """Create staging table."""
        self.logger.info(
            wrap_frame(f"Creating staging table {self.etl_info.staging_table}")
        )
        self.dumper_dest.cursor.execute(self.etl_info.staging_ddl)
        self.logger.info(
            wrap_frame(f"Staging table {self.etl_info.staging_table} created")
        )

    def drop_staging(self) -> None:
        """Drop staging table if configured."""

        if not self.staging.drop_after:
            self.logger.warning(
                wrap_frame("Staging table drop skipped by configuration")
            )
            return

        self.logger.info("Dropping staging table")
        self.dumper_dest.cursor.execute(
            f"DROP TABLE IF EXISTS {self.etl_info.staging_table}"
        )
        self.logger.info(
            wrap_frame(f"Staging table {self.etl_info.staging_table} dropped")
        )

    # Data Operations

    def load_to_staging(self) -> None:
        """Load data from source to staging table."""

        if not self.dumper_src:
            raise Error.DBHoseValueError("Source connection not configured")

        self.logger.info(wrap_frame("Loading data to staging table"))
        # ... логика загрузки ...

    def move_to_destination(self) -> None:
        """Move data from staging to destination table."""

        self.logger.info(
            wrap_frame(f"Moving data using method: {self.move_method.name}")
        )
        # ... логика перемещения ...

    # DQ Operations

    def run_dq_checks(self) -> dict[str, bool]:
        """Run configured Data Quality checks."""

        if not self.dq:
            self.logger.info("No DQ checks configured, skipping")
            return {}

        self.logger.info(wrap_frame("Running Data Quality checks"))
        # results = {}

        # ... логика DQ проверок ...


    # Public API

    def from_dbms(
        self,
        query: str | None = None,
        table: str | None = None,
    ) -> None:
        """Upload from DMBS."""

        try:
            self.create_staging()
            self.load_to_staging(query, table)
            self.run_dq_checks()
            self.move_to_destination()
        finally:
            self.drop_staging()
