from logging import Logger

from base_dumper import (
    CompressionLevel,
    CompressionMethod,
    DBConnector,
    DumpFormat,
    DumperMode,
    DumperType,
    IsolationLevel,
)

from . import errors
from .defines import FROM_CONNTYPE


def get_basehook() -> object:
    """Get Apache Airflow BaseHook."""

    try:
        from airflow.sdk.bases.hook import BaseHook # type: ignore
    except ImportError:
        try:
            from airflow.hooks.base import BaseHook # type: ignore
        except ImportError:
            from airflow.hooks.base_hook import BaseHook # type: ignore

    return BaseHook


def get_logger() -> Logger:
    """Get Apache Airflow Logger."""

    try:
        from airflow.hooks.base import log as logger # type: ignore
    except ImportError:
        from logging import getLogger
        logger = getLogger(__name__)

    return logger


def define_connector(airflow_connection: str) -> tuple[str, DBConnector]:
    """Define DBConnector from airflow_connection string."""

    basehook = get_basehook()
    connection = basehook.get_connection(airflow_connection)
    return connection.conn_type, DBConnector(
        connection.host,
        connection.schema,
        connection.login,
        connection.password,
        int(connection.port),
    )


def define_dumper(
    airflow_connection: str,
    compression_method: CompressionMethod = CompressionMethod.ZSTD,
    compression_level: int = CompressionLevel.ZSTD_DEFAULT,
    timeout: int | None = None,
    isolation: IsolationLevel = IsolationLevel.committed,
    mode: DumperMode = DumperMode.DEBUG,
    dump_format: DumpFormat = DumpFormat.BINARY,
    s3_file: bool = False,
) -> DumperType:
    """Define Dumper from airflow_connection string and additional params."""

    try:
        conn_type, db_connector = define_connector(airflow_connection)
        connection_params = FROM_CONNTYPE[conn_type]
    except KeyError:
        raise errors.DBHoseTypeError(
            f"Bad connection type \"{conn_type}\"",
        )

    connector: DBConnector = connection_params["connector"](*db_connector)
    return connection_params["dumper"](
        connector=connector,
        compression_method=compression_method,
        compression_level=compression_level,
        logger=get_logger(),
        timeout=timeout,
        isolation=isolation,
        mode=mode,
        dump_format=dump_format,
        s3_file=s3_file,
    )
