"""An Apache Airflow module for extremely fast data exchange
between DBMSs in native binary formats and CSV format."""


from base_dumper import (
    CompressionLevel,
    CompressionMethod,
    DumperMode,
    DumperType,
    DumpFormat,
    IsolationLevel,
    Timeout,
)

from .common import logo
from .core import (
    ConnectionConfig,
    ColumnMeta,
    DQCheck,
    DQConfig,
    Error,
    ETLInfo,
    MoveMethod,
    StagingConfig,
    TableMetadata,
)
from .dbhose import DBHose
from .dbhose_operator import DBHoseOperator
from .version import __version__


__all__ = (
    "__version__",
    "ColumnMeta",
    "CompressionLevel",
    "CompressionMethod",
    "ConnectionConfig",
    "DBHose",
    "DBHoseOperator",
    "DQCheck",
    "DQConfig",
    "DumperMode",
    "DumperType",
    "DumpFormat",
    "Error",
    "ETLInfo",
    "IsolationLevel",
    "MoveMethod",
    "StagingConfig",
    "TableMetadata",
    "Timeout",
)
__author__ = "0xMihalich"
__logo__ = logo()
