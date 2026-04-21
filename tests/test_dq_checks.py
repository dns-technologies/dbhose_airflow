import pytest
import logging

from dbhose_airflow import (
    DBHose,
    DQConfig,
    DQCheck,
    StagingConfig,
    DumperMode,
    DumpFormat,
)
from pgpack_dumper import PGPackDumper


test_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


class TestDQChecks:
    """Тесты Data Quality проверок."""

    @pytest.fixture(autouse=True)
    def setup_table(
        self, pg_connector, postgres_test_schema, test_rows, request
    ):
        postfix = request.node.name.replace("[", "_").replace("]", "_")[-50:]
        self.pg_table = f"{postgres_test_schema}.dq_test_{postfix}"
        pg_dumper = PGPackDumper(
            connector=pg_connector,
            mode=DumperMode.DEBUG,
            logger=test_logger,
        )
        cur = pg_dumper.cursor
        cur.execute(f"""
            CREATE TABLE {self.pg_table} (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100),
                age INTEGER
            )
        """)
        pg_dumper.from_rows(test_rows, self.pg_table)
        pg_dumper.close()

        yield

        try:
            pg_dumper = PGPackDumper(
                connector=pg_connector,
                mode=DumperMode.DEBUG,
                logger=test_logger,
            )
            cur = pg_dumper.cursor
            cur.execute(f"DROP TABLE IF EXISTS {self.pg_table}")
            pg_dumper.close()
        except Exception:
            ...

    def test_empty_check_pass(self, mock_airflow_connections):
        """Проверка empty должна пройти для непустой таблицы."""

        mock_airflow_connections
        dbhose = DBHose(
            destination_table=self.pg_table,
            destination_conn="postgres_conn",
            mode=DumperMode.DEBUG,
            dump_format=DumpFormat.BINARY,
            staging=StagingConfig(use_origin=True),  # без staging
            dq=DQConfig(disabled_checks=[], comparison_object=None),
        )
        dbhose.run_dq_checks()

    def test_disabled_check_skipped(self, mock_airflow_connections, caplog):
        """Отключенная проверка должна пропускаться с предупреждением."""

        caplog.set_level(logging.WARNING)
        mock_airflow_connections
        dbhose = DBHose(
            destination_table=self.pg_table,
            destination_conn="postgres_conn",
            mode=DumperMode.DEBUG,
            dump_format=DumpFormat.BINARY,
            staging=StagingConfig(use_origin=True),
            dq=DQConfig(
                disabled_checks=[DQCheck.empty], comparison_object=None
            ),
        )
        dbhose.run_dq_checks()

        assert "skipped by user" in caplog.text  # noqa: S101
