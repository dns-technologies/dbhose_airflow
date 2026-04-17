from native_dumper import (
    CHConnector,
    NativeDumper,
)
from pgpack_dumper import (
    PGConnector,
    PGPackDumper,
)


CLICKHOUSE = {
    "connector": CHConnector,
    "dumper": NativeDumper,
}
POSTGRES = {
    "connector": PGConnector,
    "dumper": PGPackDumper,
}
FROM_CONNTYPE = {
    "clickhouse": CLICKHOUSE,
    "ftp": CLICKHOUSE,
    "http": CLICKHOUSE,
    "postgres": POSTGRES,
    "greenplum": POSTGRES,
}
