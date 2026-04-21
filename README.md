# DBHose for Apache Airflow

```ascii
                                                                 (                )
 (  (                                                 )          )\ )     (    ( /(
 )\))(   '   (    (                   )       (     ( /(        (()/(   ( )\   )\())               (
((_)()\ )   ))\   )\    (     (      (       ))\    )\())   (    /(_))  )((_) ((_)\    (    (     ))\
_(())\_)() /((_) (( )   )\    )\     )\  '  /((_)  (_))/    )\   (_))_  ((_)_   _((_)   )\   )\   /((_)
\ \((_)/ /(_))   | |   ((_)  ((_)  _((_))  (_))    | |_    ((_)   |   \  | _ ) | || |  ((_) ((_) (_))
 \ \/\/ / / -_)  | |  / _|  / _ \ | ' ' |  / -_)   |  _|  / _ \   | |) | | _ \ | __ | / _ \ (_-< / -_)
  \_/\_/  \___|  |_|  \__|  \___/ |_|_|_|  \___|    \__|  \___/   |___/  |___/ |_||_| \___/ /__/ \___|
```

`DBHose` is an Apache Airflow module for extremely fast data exchange between DBMSs using native binary formats and CSV format.

[Documentation](https://dns-technologies.github.io/dbhose_airflow/)

## ⚠️ Project Status

**This project is in beta testing** and may contain bugs. Use with caution in production environments.

## Supported DBMS

Currently, data transfer is supported between the following databases:

- **ClickHouse**
- **Greenplum**
- **PostgreSQL**

## Description

`DBHose` is a tool for safe and efficient data movement between:
- Dump files
- Python iterables
- DataFrames (Pandas/Polars)
- Supported DBMS (ClickHouse, Greenplum, PostgreSQL)

The class includes built-in Data Quality checks and supports various data movement methods. `DBHoseOperator` provides native Airflow integration for simplified DAG development.

## Installation

```bash
pip install dbhose-airflow -U --index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

## Initialization

```python
from dbhose_airflow import DBHose

DBHose(
    destination_table: str,
    destination_conn: str | ConnectionConfig,
    source_conn: str | ConnectionConfig | None = None,
    dq_extra_conn: str | ConnectionConfig | None = None,
    *,
    source_filter: list[str] | None = None,
    staging: StagingConfig | None = None,
    move_method: MoveMethod = MoveMethod.replace,
    custom_move_sql: str | None = None,
    mode: DumperMode = DumperMode.DEBUG,
    dump_format: DumpFormat = DumpFormat.BINARY,
    dq: DQConfig | None = None,
)
```

## Parameters

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `destination_table` | `str` | Fully qualified destination table name (e.g., `"schema.table"`) |
| `destination_conn` | `str \| ConnectionConfig` | Destination connection Airflow conn_id or configuration |

### Optional Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_conn` | `str \| ConnectionConfig \| None` | `None` | Source connection (if `None`, destination is used) |
| `dq_extra_conn` | `str \| ConnectionConfig \| None` | `None` | External connection for DQ comparison table |
| `source_filter` | `list[str] \| None` | `None` | List of columns for auto generate filter expressions to insert into source table |
| `staging` | `StagingConfig \| None` | `None` | Staging table configuration |
| `move_method` | `MoveMethod` | `MoveMethod.replace` | Method for moving data from staging to destination |
| `custom_move_sql` | `str \| None` | `None` | Custom SQL for `move_method.CUSTOM` |
| `mode` | `DumperMode` | `DumperMode.DEBUG` | Operation mode (`DEBUG`, `TEST`, `PRODUCTION`) |
| `dump_format` | `DumpFormat` | `DumpFormat.BINARY` | Dump format for data transfer |
| `dq` | `DQConfig \| None` | `None` | Data Quality check configuration |

## Configuration Classes

### ConnectionConfig

Configuration for a single database connection.

```python
@dataclass
class ConnectionConfig:
    conn_id: str
    isolation: IsolationLevel = IsolationLevel.committed
    compression: CompressionMethod = CompressionMethod.ZSTD
    compression_level: int = CompressionLevel.ZSTD_DEFAULT
    timeout: int | None = None
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `conn_id` | `str` | required | Airflow connection ID |
| `isolation` | `IsolationLevel` | `committed` | Transaction isolation level |
| `compression` | `CompressionMethod` | `ZSTD` | Compression method |
| `compression_level` | `int` | `3` | Compression level |
| `timeout` | `int \| None` | `None` | Connection timeout in seconds |

### StagingConfig

Configuration for staging table behavior.

```python
@dataclass
class StagingConfig:
    drop_after: bool = True
    random_suffix: bool = True
    use_origin: bool = False
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `drop_after` | `bool` | `True` | Drop staging table after operation |
| `random_suffix` | `bool` | `True` | Add random suffix to staging table name |
| `use_origin` | `bool` | `False` | Skip staging table, write directly to destination |

### DQConfig

Configuration for Data Quality checks.

```python
@dataclass
class DQConfig:
    disabled_checks: list[DQCheck] = field(default_factory=list)
    custom_queries: list[str] = field(default_factory=list)
    exclude_columns: list[str] = field(default_factory=list)
    column_mapping: dict[str, str] = field(default_factory=dict)
    comparison_object: str | None = None
    use_source_conn: bool = False
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `disabled_checks` | `list[DQCheck]` | `[]` | List of DQ checks to skip |
| `custom_queries` | `list[str]` | `[]` | Custom DQ query paths |
| `exclude_columns` | `list[str]` | `[]` | Columns to exclude from DQ checks |
| `column_mapping` | `dict[str, str]` | `{}` | Map comparison table column names for compare with destination table column names |
| `comparison_object` | `str \| None` | `None` | Table to compare against for DQ checks |
| `use_source_conn` | `bool` | `False` | Use source connection for comparison table |

## Move Methods

| Method | Description | Requires Filter | Uses SQL |
|--------|-------------|-----------------|----------|
| `append` | Append data to existing table | No | No |
| `replace` | Replace data atomically | No | Yes |
| `delete` | Delete matching rows before insert | Yes | Yes |
| `rewrite` | Truncate and rewrite entire table | No | No |
| `custom` | Use custom SQL for move operation | No | Yes |

## DQ Checks

| Check | Description | Generates Queries | Needs Comparison |
|-------|-------------|-------------------|------------------|
| `empty` | Table is not empty | No | No |
| `uniq` | No duplicate rows | No | No |
| `future` | No dates from future | Yes | No |
| `infinity` | No infinity values | Yes | No |
| `nan` | No NaN values | Yes | No |
| `total` | Row count matches comparison | No | Yes |
| `sum` | Numeric column sums match comparison | Yes | Yes |

## Public Methods

### DBHose Class

#### `from_dbms(query: str | None = None, table: str | None = None) -> None`

Upload data from another DBMS using SQL query or direct table export.

**Parameters:**
- `query` (`str`, optional) – SQL query for data selection
- `table` (`str`, optional) – Source table name for direct export

#### `from_file(file_path: str | Path) -> None`

Upload data from a dump file.

**Parameters:**
- `file_path` (`str | Path`) – Path to the dump file

#### `from_iterable(dtype_data: Iterable[Any]) -> None`

Upload data from a Python iterable.

**Parameters:**
- `dtype_data` (`Iterable[Any]`) – Iterable containing data rows

#### `from_frame(data_frame: PdFrame | PlFrame | LfFrame) -> None`

Upload data from a Pandas or Polars DataFrame.

**Parameters:**
- `data_frame` (`PdFrame | PlFrame | LfFrame`) – DataFrame to upload

### DBHoseOperator Class

Native Airflow operator for DBHose ETL operations with template fields support.

```python
from dbhose_airflow import DBHoseOperator

DBHoseOperator(
    task_id: str,
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
    move_method: MoveMethod = MoveMethod.replace,
    custom_move_sql: str | None = None,
    mode: DumperMode = DumperMode.DEBUG,
    dump_format: DumpFormat | None = None,
    dq: DQConfig | None = None,
    **kwargs,
)
```

**Template Fields:** `destination_table`, `source_query`, `source_table`, `source_file`, `custom_move_sql`

## Usage Examples

### Basic Transfer Between Databases

```python
from dbhose_airflow import DBHose, MoveMethod

dbhose = DBHose(
    destination_table="public.users",
    destination_conn="postgres_prod",
    source_conn="postgres_stage",
    move_method=MoveMethod.replace,
)

# Transfer entire table
dbhose.from_dbms(table="users")
```

### Transfer with Filter and DQ Checks

```python
from dbhose_airflow import (
    DBHose,
    DQCheck,
    DQConfig,
    MoveMethod,
    StagingConfig,
)

dbhose = DBHose(
    destination_table="analytics.events",
    destination_conn="clickhouse_prod",
    source_conn="postgres_stage",
    source_filter=["created_at", "status"],
    staging=StagingConfig(random_suffix=True, drop_after=True),
    move_method=MoveMethod.delete,
    dq=DQConfig(
        disabled_checks=[DQCheck.future],
        exclude_columns=["password_hash"],
    ),
)

dbhose.from_dbms(query="SELECT * FROM raw_events WHERE processed = false")
```

### Using Custom Connection Configuration

```python
from dbhose_airflow import (
    DBHose,
    CompressionMethod,
    ConnectionConfig,
    IsolationLevel,
)

dest_conn = ConnectionConfig(
    conn_id="greenplum_prod",
    isolation=IsolationLevel.repeatable,
    compression=CompressionMethod.LZ4,
    compression_level=5,
    timeout=600,
)

dbhose = DBHose(
    destination_table="warehouse.facts",
    destination_conn=dest_conn,
)

dbhose.from_file("facts_dump.zst")
```

### Skip Staging Table

```python
from dbhose_airflow import DBHose, StagingConfig

dbhose = DBHose(
    destination_table="public.temp_logs",
    destination_conn="postgres_prod",
    staging=StagingConfig(use_origin=True),
)

# Writes directly to destination without staging
dbhose.from_iterable(log_rows)
```

### Custom Move SQL

```python
from dbhose_airflow import DBHose, MoveMethod

dbhose = DBHose(
    destination_table="public.metrics",
    destination_conn="postgres_prod",
    move_method=MoveMethod.custom,
    custom_move_sql="""
        MERGE INTO public.metrics AS target
        USING public.metrics_staging AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET value = source.value
        WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value);
    """,
)

dbhose.from_dbms(table="metrics")
```

### Airflow DAG with DBHoseOperator

```python
from datetime import datetime
from airflow import DAG
from dbhose_airflow import DBHoseOperator, MoveMethod, StagingConfig, DQConfig, DQCheck

with DAG(
    dag_id="dbhose_transfer",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "analytics"],
) as dag:
    
    transfer_task = DBHoseOperator(
        task_id="transfer_daily_stats",
        destination_table="analytics.daily_stats",
        destination_conn="clickhouse_prod",
        source_conn="postgres_stage",
        source_table="daily_stats",
        move_method=MoveMethod.replace,
        staging=StagingConfig(random_suffix=True, drop_after=True),
        dq=DQConfig(
            disabled_checks=[DQCheck.future],
            exclude_columns=["password_hash"],
        ),
    )
```

### Airflow DAG with PythonOperator (Legacy)

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from dbhose_airflow import DBHose, MoveMethod


def transfer_data():
    dbhose = DBHose(
        destination_table="analytics.daily_stats",
        destination_conn="clickhouse_conn",
        source_conn="postgres_conn",
        move_method=MoveMethod.replace,
    )
    dbhose.from_dbms(table="daily_stats")


with DAG(
    dag_id="daily_stats_transfer",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "analytics"],
) as dag:
    
    transfer_task = PythonOperator(
        task_id="transfer_daily_stats",
        python_callable=transfer_data,
    )
```

## Data Structures

### ColumnMeta

Metadata for a single column.

```python
class ColumnMeta(NamedTuple):
    name: str
    data_type: str
    nullable: bool
    has_default: bool
    default_value: str | None
    comment: str | None
    position: int
    type_oid: int | None
    type_namespace: int | None
    generated: str | None
    identity: str | None
```

### TableMetadata

Complete table metadata.

```python
class TableMetadata(NamedTuple):
    name: str
    schema: str
    owner: str | None
    comment: str | None
    columns: list[ColumnMeta]
    partition_by: str | None
    order_by: list[str] | None
    primary_key: list[str] | None
    engine: str
    settings: dict[str, Any] | None
    oid: int | None
```

### ETLInfo

Structure returned by DDL generation.

```python
@dataclass
class ETLInfo:
    name: str
    ddl: str
    staging_table: str
    staging_ddl: str
    table_metadata: TableMetadata
```

## ETL Pipeline Decorator

All data loading methods (`from_dbms`, `from_file`, `from_iterable`, `from_frame`) are decorated with `@etl_pipeline`, which automatically manages:

1. Staging table creation (unless `use_origin=True`)
2. Data loading
3. Data Quality checks (if configured)
4. Moving data to destination
5. Staging table cleanup (unless `drop_after=False`)

## Features

- **Automatic staging tables** – Data is loaded to staging before final destination
- **Data Quality checks** – Built-in checks before final move
- **Flexible move methods** – Multiple strategies for updating data
- **Multiple source support** – Files, DataFrames, DBMS
- **Native Airflow operator** – `DBHoseOperator` with template fields support
- **Detailed logging** – All stages are logged with visual framing
- **Automatic format detection** – BINARY or CSV format auto-selected based on source/destination compatibility
- **Cross-platform column mapping** – Unified column metadata across PostgreSQL and ClickHouse
- **Cython-optimized DDL generation** – Fast metadata extraction and staging DDL generation

## Beta Version Limitations

- Only ClickHouse, Greenplum, and PostgreSQL are supported
- Some edge cases with complex data types may not be fully tested
- API may change in future versions
- Documentation may be incomplete

## Requirements

- Apache Airflow >= 2.0
- Python >= 3.10
- native-dumper (for ClickHouse)
- pgpack-dumper (for PostgreSQL/Greenplum)
- dr-herriot (for dumps manipulations)

## Error Handling

All exceptions are wrapped in `DBHoseError` hierarchy:

- `DBHoseError` – Base exception
- `DBHoseValueError` – Invalid configuration or parameters
- `DBHoseTypeError` – Type mismatches
- `DBHoseNotFoundError` – Missing objects
- `DBHosePermissionError` – Permission issues

## Reporting Issues

When encountering bugs or unexpected behavior, please report them to help improve the project's stability.

### License

MIT
