from airflow.hooks.base import (
    BaseHook,
    log as logger,
)
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


def define_connector(airflow_connection: str) -> tuple[str, DBConnector]:
    """Define DBConnector from airflow_connection string."""

    connection = BaseHook.get_connection(airflow_connection)
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
        raise errors.DBHoseAirflowTypeError(
            f"Bad connection type \"{conn_type}\"",
        )

    connector: DBConnector = connection_params["connector"](*db_connector)
    return connection_params["dumper"](
        connector=connector,
        compression_method=compression_method,
        compression_level=compression_level,
        logger=logger,
        timeout=timeout,
        isolation=isolation,
        mode=mode,
        dump_format=dump_format,
        s3_file=s3_file,
    )
