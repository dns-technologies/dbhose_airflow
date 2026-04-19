import pytest
import logging

from native_dumper import NativeDumper
from base_dumper import DumperMode


test_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


class TestDataFrame:
    """Тесты загрузки из pandas DataFrame."""

    @pytest.fixture(autouse=True)
    def setup_table(self, ch_connector, request, clickhouse_test_db):

        postfix = request.node.name.replace("[", "_").replace("]", "_")[-50:]
        self.ch_table = f"{clickhouse_test_db}.df_test_{postfix}"
        ch_dumper = NativeDumper(
            connector=ch_connector,
            logger=test_logger,
            mode=DumperMode.DEBUG,
        )

        with ch_dumper.cursor as cur:
            cur.execute(f"""
                CREATE TABLE {self.ch_table} (
                    id Int32,
                    name String,
                    age Int32
                ) ENGINE = Memory
            """)
        ch_dumper.close()

        yield

        try:
            ch_dumper = NativeDumper(
                connector=ch_connector,
                mode=DumperMode.DEBUG,
                logger=test_logger,
            )
            with ch_dumper.cursor as cur:
                cur.execute(f"DROP TABLE IF EXISTS {self.ch_table}")
            ch_dumper.close()
        except Exception:
            ...

    def test_from_dataframe(
        self,
        dbhose_pg_to_ch,
        test_dataframe,
    ):
        """Загрузка из pandas DataFrame в ClickHouse."""
        dbhose_pg_to_ch.destination_table = self.ch_table
        dbhose_pg_to_ch.target_table = self.ch_table
        dbhose_pg_to_ch.staging.use_origin = True  # пишем напрямую

        dbhose_pg_to_ch.from_frame(test_dataframe)

        # Проверяем данные
        reader = dbhose_pg_to_ch.dumper_dest.to_reader(
            f"SELECT id, name, age FROM {self.ch_table} ORDER BY id"
        )
        results = list(reader.to_rows())

        assert len(results) == len(test_dataframe)  # noqa: S101
        for i, row in enumerate(results):
            assert row[0] == test_dataframe.iloc[i]["id"]  # noqa: S101
            assert row[1] == test_dataframe.iloc[i]["name"]  # noqa: S101
            assert row[2] == test_dataframe.iloc[i]["age"]  # noqa: S101
