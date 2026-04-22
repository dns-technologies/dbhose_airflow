from abc import (
    ABC,
    abstractmethod,
)
from re import (
    Match,
    compile,
)
from typing import (
    Any,
    NoReturn,
)

from base_dumper import DumperType
from native_dumper import NativeDumper
from pgpack_dumper import PGPackDumper

from . import errors
from .structs import (
    ETLInfo,
    MoveMethod,
    TableMetadata,
)


FIND_PARTITION = compile(r"\((\w+)\)")


class MoveStrategy(ABC):
    """Base strategy for moving data from staging to destination."""

    custom_sql: str
    dumper_dest: DumperType
    etl_info: ETLInfo
    source_filter: list[str]
    metadata: TableMetadata
    method: MoveMethod

    def __init__(
        self,
        custom_sql: str | None,
        dumper_dest: DumperType,
        etl_info: ETLInfo,
        source_filter: list[str] | None = None,
        method: MoveMethod = MoveMethod.AUTO,
    ):
        """Class initialization."""

        self.custom_sql = custom_sql
        self.dumper_dest = dumper_dest
        self.etl_info = etl_info
        self.source_filter = source_filter or []
        self.metadata = etl_info.table_metadata
        self.method = method

    @abstractmethod
    def get_target_table(self) -> str:
        """Return which staging table to use."""

    @abstractmethod
    def get_ddl(self) -> str:
        """Return which DDL to use for staging table."""

    @abstractmethod
    def execute(self) -> None:
        """Execute the move strategy."""

    def _is_clickhouse(self) -> bool:
        """Check if destination is ClickHouse."""

        return isinstance(self.dumper_dest, NativeDumper)

    def _is_postgres(self) -> bool:
        """Check if destination is PostgreSQL/Greenplum."""

        return isinstance(self.dumper_dest, PGPackDumper)

    def _extract_partition_column(self) -> str:
        """Extract partition column from partition_by expression."""
        match: Match = FIND_PARTITION.search(self.metadata.partition_by)
        return match.group(1) if match else self.metadata.partition_by

class AppendStrategy(MoveStrategy):
    """Simple INSERT from staging to destination."""

    def get_target_table(self) -> str:
        return self.etl_info.staging_temp

    def get_ddl(self) -> str:
        return self.etl_info.staging_ddl_temp

    def execute(self) -> None:
        if self._is_clickhouse():
            columns = ", ".join(
                f"`{col.name}`" for col in self.metadata.columns
            )
        elif self._is_postgres():
            columns = ", ".join(
                f'"{col.name}"' for col in self.metadata.columns
            )
        else:
            raise errors.DBHoseNotFoundError("Unsupported database.")

        sql = f"""
            INSERT INTO {self.etl_info.name} ({columns})
            SELECT {columns} FROM {self.get_target_table()}
        """
        self.dumper_dest.cursor.execute(sql)


class RewriteStrategy(MoveStrategy):
    """TRUNCATE + INSERT."""

    def get_target_table(self) -> str:
        return self.etl_info.staging_temp

    def get_ddl(self) -> str:
        return self.etl_info.staging_ddl_temp

    def execute(self) -> None:
        self.dumper_dest.cursor.execute(
            f"TRUNCATE TABLE {self.etl_info.name}",
        )
        AppendStrategy(
            self.custom_sql,
            self.dumper_dest,
            self.etl_info,
            self.source_filter,
            self.method,
        ).execute()


class DeleteStrategy(MoveStrategy):
    """DELETE matching rows + INSERT."""

    def get_target_table(self) -> str:
        return self.etl_info.staging_table

    def get_ddl(self) -> str:
        return self.etl_info.staging_ddl_simple

    def execute(self) -> None:
        if not self.source_filter:
            raise errors.DBHoseValueError(
                "source_filter required for DELETE strategy",
            )

        if self._is_clickhouse():
            self._execute_clickhouse()
        else:
            self._execute_postgres()

        AppendStrategy(
            self.custom_sql,
            self.dumper_dest,
            self.etl_info,
            self.source_filter,
            self.method,
        ).execute()

    def _execute_clickhouse(self) -> None:
        if self.metadata.partition_by:
            self._execute_clickhouse_partitioned()
        else:
            self._execute_clickhouse_simple()

    def _execute_clickhouse_partitioned(self) -> None:
        partition_col = self._extract_partition_column()
        sql = f"""
            SELECT DISTINCT {partition_col}
            FROM {self.etl_info.staging_table}
        """
        reader = self.dumper_dest.to_reader(sql)
        partitions = [row[0] for row in reader.to_rows()]
        filter_cols = ", ".join(self.source_filter)

        for partition in partitions:
            sql = f"""
                ALTER TABLE {self.etl_info.name}
                DELETE IN PARTITION {partition}
                WHERE ({filter_cols}) IN (
                    SELECT {filter_cols}
                    FROM {self.etl_info.staging_table}
                )
            """
            self.dumper_dest.cursor.execute(sql)

    def _execute_clickhouse_simple(self) -> None:
        filter_cols = ", ".join(self.source_filter)
        sql = f"""
            ALTER TABLE {self.etl_info.name}
            DELETE WHERE ({filter_cols}) IN (
                SELECT {filter_cols}
                FROM {self.etl_info.staging_table}
            )
        """
        self.dumper_dest.cursor.execute(sql)

    def _execute_postgres(self) -> None:
        join_conditions = " AND ".join(
            f'dest."{col}" = src."{col}"' for col in self.source_filter
        )
        sql = f"""
            DELETE FROM {self.etl_info.name} AS dest
            USING {self.etl_info.staging_table} AS src
            WHERE {join_conditions}
        """
        self.dumper_dest.cursor.execute(sql)


class ReplaceStrategy(MoveStrategy):
    """REPLACE PARTITION / ATTACH PARTITION."""

    def get_target_table(self) -> str:
        return self.etl_info.staging_table

    def get_ddl(self) -> str:
        return self.etl_info.staging_ddl

    def execute(self) -> None:
        if not self.metadata.partition_by:
            raise errors.DBHoseValueError(
                "Table must be partitioned for REPLACE strategy",
            )

        if self._is_clickhouse():
            self._execute_clickhouse()
        else:
            self._execute_postgres()

    def _format_partition_value(self, value: Any) -> str:
        """Format partition value for ClickHouse SQL."""

        if value is None:
            return "tuple()"

        if isinstance(value, str):
            return f"'{value}'"

        if isinstance(value, (tuple, list)):
            value = ",".join(self._format_partition_value(v) for v in value)
            return f"({value})"

        return str(value)

    def _execute_clickhouse(self) -> None:
        partition_col = self._extract_partition_column()
        staging_name = self.etl_info.staging_table.split('.')[-1]
        sql = f"""
            SELECT DISTINCT {partition_col}
            FROM {self.etl_info.staging_table}
        """
        reader = self.dumper_dest.to_reader(sql)
        partitions = [row[0] for row in reader.to_rows()]

        for p in partitions:
            sql = f"""
                ALTER TABLE {self.etl_info.name}
                REPLACE PARTITION {self._format_partition_value(p)}
                FROM `{staging_name}`
            """
            self.dumper_dest.cursor.execute(sql)

    def _execute_postgres(self) -> None:
        partition_col = self.metadata.partition_by.replace(
            "RANGE (", ""
        ).replace(")", "").strip("; \t\n\r")
        sql = f"""
            SELECT
                MIN({partition_col}) as min_val,
                MAX({partition_col}) as max_val
            FROM {self.etl_info.staging_table}
        """
        reader = self.dumper_dest.to_reader(sql)
        row = next(iter(reader.to_rows()))
        min_val, max_val = row[0], row[1]

        if min_val is None:
            return

        temp_part = f"{self.metadata.name}_part_{min_val}_{max_val}".replace(
            "-", "_"
        )

        try:
            self.dumper_dest.cursor.execute(
                f"ALTER TABLE {self.etl_info.name} "
                f"DETACH PARTITION {temp_part}"
            )
        except Exception:
            ...

        self.dumper_dest.cursor.execute(
            f"ALTER TABLE {self.etl_info.name} "
            f"ATTACH PARTITION {self.etl_info.staging_table} "
            f"FOR VALUES FROM ('{min_val}') TO ('{max_val}')"
        )
        self.dumper_dest.cursor.execute(
            f"ALTER TABLE {self.etl_info.staging_table} RENAME TO {temp_part}"
        )


class AutoStrategy(MoveStrategy):
    """Automatically select best strategy."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._strategy = self._determine_strategy()

    def _determine_strategy(self) -> MoveStrategy:
        if self.source_filter:
            return DeleteStrategy(
                self.custom_sql,
                self.dumper_dest,
                self.etl_info,
                self.source_filter,
                self.method,
            )

        if self.metadata.partition_by:
            return ReplaceStrategy(
                self.custom_sql,
                self.dumper_dest,
                self.etl_info,
                self.source_filter,
                self.method,
            )

        return RewriteStrategy(
            self.custom_sql,
            self.dumper_dest,
            self.etl_info,
            self.source_filter,
            self.method,
        )

    def get_target_table(self) -> str:
        return self._strategy.get_target_table()

    def get_ddl(self) -> str:
        return self._strategy.get_ddl()

    def execute(self) -> None:
        self._strategy.execute()


class CustomStrategy(MoveStrategy):
    """Custom SQL strategy."""

    def __init__(self, custom_sql: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.custom_sql = custom_sql

    def get_target_table(self) -> str:
        return self.etl_info.staging_table

    def get_ddl(self) -> str:
        return self.etl_info.staging_ddl

    def execute(self) -> None:
        self.dumper_dest.cursor.execute(self.custom_sql)


def move_strategy_error(method: MoveMethod, *_) -> NoReturn:
    """Unknown strategy error."""

    raise errors.DBHoseValueError(f"Unknown move method: {method}")


MOVE_STRATEGY = {
    MoveMethod.APPEND: AppendStrategy,
    MoveMethod.AUTO: AutoStrategy,
    MoveMethod.CUSTOM: CustomStrategy,
    MoveMethod.DELETE: DeleteStrategy,
    MoveMethod.REPLACE: ReplaceStrategy,
    MoveMethod.REWRITE: RewriteStrategy,
}


def get_move_strategy(
    method: MoveMethod,
    dumper_dest: DumperType,
    etl_info: ETLInfo,
    source_filter: list[str] | None = None,
    custom_sql: str | None = None,
) -> MoveStrategy:
    """Factory for creating move strategies."""

    if method is MoveMethod.CUSTOM and not custom_sql:
        raise errors.DBHoseValueError("custom_sql required for CUSTOM method")

    return MOVE_STRATEGY.get(method, move_strategy_error)(
        custom_sql,
        dumper_dest,
        etl_info,
        source_filter,
        method,
    )
