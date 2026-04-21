import pytest
import logging
import re

from pgpack_dumper import PGPackDumper
from native_dumper import NativeDumper
from dbhose_airflow import (
    DBHose,
    DumperMode,
    MoveMethod,
    StagingConfig,
    DumpFormat,
)


test_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


def safe_name(name: str) -> str:
    """Заменяет все небуквенно-цифровые символы на _"""

    return re.sub(r"[^a-zA-Z0-9]", "_", name)


class TestPGtoClickHouse:
    """Тесты переноса данных из PostgreSQL в ClickHouse."""

    @pytest.fixture(autouse=True)
    def setup_tables(
        self,
        pg_connector,
        ch_connector,
        postgres_test_schema,
        clickhouse_test_db,
        test_rows,
        request,
    ):
        """Создает тестовые таблицы в PG и CH."""
        postfix = safe_name(request.node.name)[-50:]
        self.pg_connector = pg_connector
        self.ch_connector = ch_connector
        self.pg_table_name = f"pg_src_{postfix}"
        self.pg_table_full = f"{postgres_test_schema}.{self.pg_table_name}"
        pg_dumper = PGPackDumper(
            pg_connector, mode=DumperMode.DEBUG, logger=test_logger
        )
        cur = pg_dumper.cursor
        cur.execute(f"""
            CREATE TABLE {self.pg_table_full} (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                age INTEGER
            )
        """)
        pg_dumper.from_rows(test_rows, self.pg_table_full)
        pg_dumper.close()
        self.ch_table_name = f"ch_dest_{postfix}"
        self.ch_table_full = f"{clickhouse_test_db}.{self.ch_table_name}"
        ch_dumper = NativeDumper(
            ch_connector, mode=DumperMode.DEBUG, logger=test_logger
        )
        cur = ch_dumper.cursor
        cur.execute(f"""
            CREATE TABLE {self.ch_table_full} (
                id Int32,
                name String,
                age Int32
            ) ENGINE = MergeTree
            PARTITION BY name
            ORDER BY id
        """)
        ch_dumper.close()

        yield

        try:
            pg_dumper = PGPackDumper(
                pg_connector, mode=DumperMode.DEBUG, logger=test_logger
            )
            cur = pg_dumper.cursor
            cur.execute(f"DROP TABLE IF EXISTS {self.pg_table_full}")
            pg_dumper.close()
        except Exception:
            ...

        try:
            ch_dumper = NativeDumper(
                ch_connector, mode=DumperMode.DEBUG, logger=test_logger
            )
            cur = ch_dumper.cursor
            cur.execute(f"DROP TABLE IF EXISTS {self.ch_table_full}")
            ch_dumper.close()
        except Exception:
            ...

    def test_basic_transfer(
        self, mock_airflow_connections, test_rows
    ):
        """Базовый перенос данных из PG в CH."""

        _ = mock_airflow_connections
        dbhose = DBHose(
            destination_table=self.ch_table_full,
            destination_conn="clickhouse_conn",
            source_conn="postgres_conn",
            mode=DumperMode.DEBUG,
            staging=StagingConfig(use_origin=True),
        )
        dbhose.from_dbms(
            table=self.pg_table_full
        )
        reader = dbhose.dumper_dest.to_reader(
            f"SELECT id, name, age FROM {self.ch_table_full} ORDER BY id"
        )
        results = list(reader.to_rows())
        assert len(results) == len(test_rows)  # noqa: S101

        for result, expected in zip(results, test_rows):
            assert result[0] == expected[0]  # noqa: S101
            assert result[1] == expected[1]  # noqa: S101
            assert result[2] == expected[2]  # noqa: S101

    @pytest.mark.parametrize(
        "move_method", [MoveMethod.append, MoveMethod.rewrite]
    )
    def test_move_methods(
        self,
        mock_airflow_connections,
        move_method,
        test_rows,
    ):
        """Тест разных методов перемещения."""

        _ = mock_airflow_connections
        dbhose = DBHose(
            destination_table=self.ch_table_full,
            destination_conn="clickhouse_conn",
            source_conn="postgres_conn",
            mode=DumperMode.DEBUG,
            staging=StagingConfig(drop_after=True, random_suffix=True),
            move_method=move_method,
        )
        dbhose.from_dbms(table=self.pg_table_full)
        reader = dbhose.dumper_dest.to_reader(
            f"SELECT COUNT(*) FROM {self.ch_table_full}"
        )
        rows = list(reader.to_rows())
        print(rows)
        count = rows[0][0]
        assert count == len(test_rows)  # noqa: S101

    def test_with_staging(
        self, mock_airflow_connections, test_rows
    ):
        """Тест с явным использованием staging таблицы."""

        _ = mock_airflow_connections
        dbhose = DBHose(
            destination_table=self.ch_table_full,
            destination_conn="clickhouse_conn",
            source_conn="postgres_conn",
            mode=DumperMode.DEBUG,
            dump_format=DumpFormat.BINARY,
            staging=StagingConfig(drop_after=True, random_suffix=True),
        )
        dbhose.from_dbms(table=self.pg_table_full)
        reader = dbhose.dumper_dest.to_reader(
            f"SELECT COUNT(*) FROM {self.ch_table_full}"
        )
        count = list(reader.to_rows())[0][0]
        assert count == len(test_rows)  # noqa: S101
