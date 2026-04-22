from collections.abc import Iterable
from pathlib import Path
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context
from base_dumper import (
    DumperMode,
    DumpFormat,
)
from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
)

from .core import (
    ConnectionConfig,
    DQConfig,
    Error,
    MoveMethod,
    StagingConfig,
)
from .dbhose import DBHose


class DBHoseOperator(BaseOperator):
    """
    Airflow Operator for DBHose ETL operations.

    Executes data transfer using DBHose with full support for:
    - Staging tables
    - Data Quality checks
    - Multiple move methods
    - Various source types (DBMS, files, iterables, DataFrames)

    :param destination_table: Fully qualified destination table name
    :param destination_conn: Destination connection Airflow conn_id
    :param source_type: Type of source ('dbms', 'file', 'iterable', 'frame')
    :param source_conn: Source connection Airflow conn_id (for 'dbms' source)
    :param source_query: SQL query for source data (for 'dbms' source)
    :param source_table: Source table name (for 'dbms' source)
    :param source_file: Path to dump file (for 'file' source)
    :param source_iterable: Iterable data (for 'iterable' source)
    :param source_frame: DataFrame (for 'frame' source)
    :param dq_extra_conn: External connection for DQ comparison
    :param source_filter: List of WHERE conditions for source query
    :param staging: Staging table configuration
    :param move_method: Method for moving data from staging to destination
    :param custom_move_sql: Custom SQL for move_method.CUSTOM
    :param mode: Operation mode (DEBUG, TEST, PROD)
    :param dump_format: Override dump format (auto-detected if None)
    :param dq: Data Quality check configuration
    :param dbhose_kwargs: Additional keyword arguments for DBHose
    """

    template_fields = (
        "destination_table",
        "source_query",
        "source_table",
        "source_file",
        "custom_move_sql",
    )

    def __init__(
        self,
        *,
        destination_table: str,
        destination_conn: str | ConnectionConfig,
        source_type: str = "dbms",
        source_conn: str | ConnectionConfig | None = None,
        source_query: str | None = None,
        source_table: str | None = None,
        source_file: str | Path | None = None,
        source_iterable: Iterable[Any] | None = None,
        source_frame: PdFrame | PlFrame | LfFrame | None = None,
        dq_extra_conn: str | ConnectionConfig | None = None,
        source_filter: list[str] | None = None,
        staging: StagingConfig | None = None,
        move_method: MoveMethod = MoveMethod.AUTO,
        custom_move_sql: str | None = None,
        mode: DumperMode = DumperMode.DEBUG,
        dump_format: DumpFormat | None = None,
        dq: DQConfig | None = None,
        **kwargs,
    ) -> None:
        """Initialize DBHose operator."""

        super().__init__(**kwargs)
        self.destination_table = destination_table
        self.destination_conn = destination_conn
        self.source_type = source_type
        self.source_conn = source_conn
        self.source_query = source_query
        self.source_table = source_table
        self.source_file = source_file
        self.source_iterable = source_iterable
        self.source_frame = source_frame
        self.dq_extra_conn = dq_extra_conn
        self.source_filter = source_filter
        self.staging = staging
        self.move_method = move_method
        self.custom_move_sql = custom_move_sql
        self.mode = mode
        self.dump_format = dump_format
        self.dq = dq

    def execute(self, context: Context) -> Any:
        """
        Execute the DBHose transfer.

        :param context: Airflow context dictionary
        """

        _ = context
        dbhose = DBHose(
            destination_table=self.destination_table,
            destination_conn=self.destination_conn,
            source_conn=self.source_conn,
            dq_extra_conn=self.dq_extra_conn,
            source_filter=self.source_filter,
            staging=self.staging,
            move_method=self.move_method,
            custom_move_sql=self.custom_move_sql,
            mode=self.mode,
            dump_format=self.dump_format,
            dq=self.dq,
        )

        if self.source_type == "dbms":
            dbhose.from_dbms(
                query=self.source_query,
                table=self.source_table,
            )
        elif self.source_type == "file":
            if not self.source_file:
                raise Error.DBHoseValueError(
                    "source_file is required for source_type='file'"
                )
            dbhose.from_file(self.source_file)
        elif self.source_type == "iterable":
            if self.source_iterable is None:
                raise Error.DBHoseValueError(
                    "source_iterable is required for source_type='iterable'"
                )
            dbhose.from_iterable(self.source_iterable)
        elif self.source_type == "frame":
            if self.source_frame is None:
                raise Error.DBHoseValueError(
                    "source_frame is required for source_type='frame'"
                )
            dbhose.from_frame(self.source_frame)
        else:
            raise Error.DBHoseValueError(
                f"Unknown source_type: {self.source_type}. "
                "Must be one of: 'dbms', 'file', 'iterable', 'frame'"
            )

        self.log.info(
            f"Successfully transferred data to {self.destination_table}"
        )
