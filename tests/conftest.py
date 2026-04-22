import pytest
import docker
import time
import random
import string
import logging

import pandas as pd
import psycopg
import requests
from docker import DockerClient

from unittest.mock import MagicMock, patch

from native_dumper import CHConnector, NativeDumper
from pgpack_dumper import PGConnector, PGPackDumper
from dbhose_airflow import (
    DBHose,
    StagingConfig,
    MoveMethod,
    DumperMode,
    DumpFormat,
)


test_logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)
CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:latest"
POSTGRES_IMAGE = "postgres:15"


def get_basehook_path() -> str:
    """Определить правильный путь к BaseHook для мока."""
    try:
        from airflow.sdk.bases.hook import BaseHook  # type: ignore  # noqa: F401
        return "airflow.sdk.bases.hook.BaseHook"
    except (ImportError, AttributeError):
        try:
            from airflow.hooks.base import BaseHook  # type: ignore  # noqa: F401
            return "airflow.hooks.base.BaseHook"
        except (ImportError, AttributeError):
            return "airflow.hooks.base_hook.BaseHook"


def random_string(length: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))  # noqa: S311


def wait_for_clickhouse(host: str, port: int, timeout: int = 30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"http://{host}:{port}/ping", timeout=2)
            if response.status_code == 200:
                return True
        except Exception:
            time.sleep(1)
    raise TimeoutError(f"ClickHouse not ready after {timeout} seconds")


def wait_for_postgres(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    timeout: int = 30,
):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            conn = psycopg.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=database,
                connect_timeout=2,
            )
            conn.close()
            return True
        except Exception:
            time.sleep(1)
    raise TimeoutError(f"PostgreSQL not ready after {timeout} seconds")


@pytest.fixture(scope="session")
def docker_client():
    client = docker.from_env()
    yield client
    client.close()


@pytest.fixture(scope="session")
def clickhouse_container(docker_client: DockerClient):
    container_name = f"ch_test_{random_string()}"
    container = docker_client.containers.run(
        CLICKHOUSE_IMAGE,
        name=container_name,
        environment={
            "CLICKHOUSE_USER": "testuser",
            "CLICKHOUSE_PASSWORD": "testpass",
            "CLICKHOUSE_DB": "testdb",
        },
        ports={"8123/tcp": None, "9000/tcp": None},
        detach=True,
        remove=False,
    )
    time.sleep(5)
    container.reload()
    http_port = container.attrs["NetworkSettings"]["Ports"]["8123/tcp"][0][
        "HostPort"
    ]
    native_port = container.attrs["NetworkSettings"]["Ports"]["9000/tcp"][0][
        "HostPort"
    ]
    wait_for_clickhouse("localhost", int(http_port))

    yield {
        "host": "localhost",
        "http_port": int(http_port),
        "native_port": int(native_port),
        "user": "testuser",
        "password": "testpass",
        "database": "testdb",
    }
    try:
        container.stop(timeout=10)
        container.remove()
    except Exception as e:
        test_logger.warning(f"Failed to remove ClickHouse container: {e}")


@pytest.fixture(scope="session")
def postgres_container(docker_client):
    container_name = f"pg_test_{random_string()}"
    container = docker_client.containers.run(
        POSTGRES_IMAGE,
        name=container_name,
        environment={
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass",
            "POSTGRES_DB": "testdb",
        },
        ports={"5432/tcp": None},
        detach=True,
        remove=False,
    )
    time.sleep(3)
    container.reload()
    host_port = container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0][
        "HostPort"
    ]
    wait_for_postgres(
        "localhost", int(host_port), "testuser", "testpass", "testdb"
    )

    yield {
        "host": "localhost",
        "port": int(host_port),
        "user": "testuser",
        "password": "testpass",
        "database": "testdb",
    }
    try:
        container.stop(timeout=10)
        container.remove()
    except Exception as e:
        test_logger.warning(f"Failed to remove PostgreSQL container: {e}")


@pytest.fixture(scope="session")
def clickhouse_test_db(clickhouse_container):
    """Создает уникальную тестовую базу данных в ClickHouse."""
    db_name = f"test_db_{random_string()}"

    system_connector = CHConnector(
        host=clickhouse_container["host"],
        port=clickhouse_container["http_port"],
        user=clickhouse_container["user"],
        password=clickhouse_container["password"],
    )

    dumper = NativeDumper(system_connector, logger=test_logger)
    cur = dumper.cursor
    cur.execute(f"CREATE DATABASE {db_name}")
    dumper.close()

    yield db_name

    dumper = NativeDumper(system_connector, logger=test_logger)
    cur = dumper.cursor
    cur.execute(f"DROP DATABASE IF EXISTS {db_name}")
    dumper.close()


@pytest.fixture(scope="session")
def postgres_test_schema(postgres_container):
    """Создает уникальную тестовую схему в PostgreSQL."""
    schema_name = f"test_schema_{random_string()}"

    pg_connector = PGConnector(
        host=postgres_container["host"],
        port=postgres_container["port"],
        user=postgres_container["user"],
        password=postgres_container["password"],
        dbname=postgres_container["database"],
    )

    dumper = PGPackDumper(pg_connector, logger=test_logger)
    cur = dumper.cursor
    cur.execute(f"CREATE SCHEMA {schema_name}")
    dumper.close()

    yield schema_name

    dumper = PGPackDumper(pg_connector, logger=test_logger)
    cur = dumper.cursor
    cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
    dumper.close()


@pytest.fixture(scope="session")
def pg_connector(postgres_container):
    """Создает PGConnector для тестовой схемы."""
    return PGConnector(
        host=postgres_container["host"],
        port=postgres_container["port"],
        user=postgres_container["user"],
        password=postgres_container["password"],
        dbname=postgres_container["database"],
    )


@pytest.fixture(scope="session")
def ch_connector(clickhouse_container, clickhouse_test_db):
    """Создает CHConnector для тестовой базы данных."""
    return CHConnector(
        host=clickhouse_container["host"],
        port=clickhouse_container["http_port"],
        user=clickhouse_container["user"],
        password=clickhouse_container["password"],
        dbname=clickhouse_test_db,
    )


@pytest.fixture
def test_rows():
    return [
        (1, "Alice", 30),
        (2, "Bob", 25),
        (3, "Charlie", 35),
        (4, "Diana", 28),
        (5, "Eve", 32),
    ]


@pytest.fixture
def test_dataframe(test_rows):
    return pd.DataFrame(test_rows, columns=["id", "name", "age"])


@pytest.fixture
def mock_airflow_connections(pg_connector, ch_connector):
    """Мокает BaseHook.get_connection для обеих БД."""

    def mock_get_connection(conn_id):
        if conn_id == "postgres_conn":
            mock_conn = MagicMock()
            mock_conn.conn_type = "postgres"
            mock_conn.conn_id = "postgres_conn"
            mock_conn.host = pg_connector.host
            mock_conn.port = pg_connector.port
            mock_conn.login = pg_connector.user
            mock_conn.password = pg_connector.password
            mock_conn.schema = pg_connector.dbname
            return mock_conn
        elif conn_id == "clickhouse_conn":
            mock_conn = MagicMock()
            mock_conn.conn_type = "clickhouse"
            mock_conn.conn_id = "clickhouse_conn"
            mock_conn.host = ch_connector.host
            mock_conn.port = ch_connector.port
            mock_conn.login = ch_connector.user
            mock_conn.password = ch_connector.password
            mock_conn.schema = ch_connector.dbname
            return mock_conn
        raise ValueError(f"Unknown conn_id: {conn_id}")

    basehook_path = get_basehook_path()
    with patch(
        f"{basehook_path}.get_connection",
        side_effect=mock_get_connection,
    ):
        yield


@pytest.fixture
def dbhose_pg_to_ch(
    pg_connector,
    ch_connector,
    postgres_test_schema,
    test_rows,
    request,
):
    """Создает тестовую таблицу в PG и возвращает настроенный DBHose."""
    move_method = getattr(request, "param", {}).get(
        "move_method", MoveMethod.AUTO
    )
    staging = getattr(request, "param", {}).get(
        "staging", StagingConfig(drop_after=True, random_suffix=True)
    )

    pg_table = f"{postgres_test_schema}.pg_src_{random_string()}"
    pg_dumper = PGPackDumper(
        pg_connector, mode=DumperMode.DEBUG, logger=test_logger
    )
    cur = pg_dumper.cursor
    cur.execute(f"""
        CREATE TABLE {pg_table} (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            age INTEGER
        )
    """)
    pg_dumper.from_rows(test_rows, pg_table)
    pg_dumper.close()

    basehook_path = get_basehook_path()
    with patch(f"{basehook_path}.get_connection") as mock_get:
        mock_pg = MagicMock()
        mock_pg.conn_type = "postgres"
        mock_pg.conn_id = "postgres_conn"
        mock_pg.host = pg_connector.host
        mock_pg.port = pg_connector.port
        mock_pg.login = pg_connector.user
        mock_pg.password = pg_connector.password
        mock_pg.schema = pg_connector.dbname
        mock_ch = MagicMock()
        mock_ch.conn_type = "clickhouse"
        mock_ch.conn_id = "clickhouse_conn"
        mock_ch.host = ch_connector.host
        mock_ch.port = ch_connector.port
        mock_ch.login = ch_connector.user
        mock_ch.password = ch_connector.password
        mock_ch.schema = ch_connector.dbname
        mock_get.side_effect = lambda conn_id: {
            "postgres_conn": mock_pg,
            "clickhouse_conn": mock_ch,
        }[conn_id]
        ch_table = f"ch_dest_{random_string()}"
        full_ch_table = f"{ch_connector.dbname}.{ch_table}"
        ch_dumper = NativeDumper(
            ch_connector, mode=DumperMode.DEBUG, logger=test_logger
        )
        cur = ch_dumper.cursor
        cur.execute(f"""
            CREATE TABLE {full_ch_table} (
                id Int32,
                name String,
                age Int32
            ) ENGINE = MergeTree
            ORDER BY id
        """)
        ch_dumper.close()
        dbhose = DBHose(
            destination_table=full_ch_table,
            destination_conn="clickhouse_conn",
            source_conn="postgres_conn",
            staging=staging,
            move_method=move_method,
            mode=DumperMode.DEBUG,
            dump_format=DumpFormat.BINARY,
        )

        yield dbhose

        try:
            pg_dumper = PGPackDumper(
                pg_connector, mode=DumperMode.DEBUG, logger=test_logger
            )
            cur = pg_dumper.cursor
            cur.execute(f"DROP TABLE IF EXISTS {pg_table}")
            pg_dumper.close()
        except Exception:
            ...

        try:
            ch_dumper = NativeDumper(
                ch_connector, mode=DumperMode.DEBUG, logger=test_logger
            )
            cur = ch_dumper.cursor
            cur.execute(f"DROP TABLE IF EXISTS {full_ch_table}")
            ch_dumper.close()
        except Exception:
            ...
