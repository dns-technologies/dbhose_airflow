# Version History

## 0.2.0.dev4

* Developer release (not public to pip)
* Change `ETLInfo` structure
* Change `ddl.pyx`
* Fix DumpFormat autodetection
* Fix CSV mode

## 0.2.0.dev3

* Developer release (not public to pip)
* Change `DQConfig` structure
* Rename `DQConfig`.`comparison_table` to `DQConfig`.`comparison_object`
* Fix Clickhouse DDL parsing
* Add DQ test description output for error tests

## 0.2.0.dev2

* Developer release (not public to pip)
* Add `DBHoseOperator`
* Fix `DBHose.dump_format` parameter
* Refactor `DBHose._initialize()` method
* Move errors.py, ddl.py and structs.py into core
* Change language `generate_ddl()` function to Cython
* Rewrite Python ddl.py to Cython ddl.pyx
* Update `README.md`

## 0.2.0.dev1

* Developer release (not public to pip)
* Update `README.md`
* Add `DBHose.error_message()` NoReturn method
* Code refactor
* Change `DBHose.dump_format` to property method
* Change `DBHose.mode` to property method
* Change `DBHose._init_conn()` method from staticmethod into classmethod
* Refactor `DBHose._initialize()` method
* Add auto change `DBHose.dump_format` after uploading stage to `DumpFormat.BINARY`
* Change `DBHose.from_file()` method parameter from fileobj: `io.BufferedReader` to file_path: `str` | `pathlib.Path`
* Improve `ConnectionConfig` dataclass
* Fix dumpers initialization
* Fix DQ tests
* Rename `DBHose._fetch_single_value()` to `DBHose._fetch_dq_values()`
* Fix move queries
* Fix some bugs
* Fix work with all Apache Airflow versions
* Add basic pytests

## 0.2.0.dev0

* Developer release (not public to pip)
* Remove depends dbhose-utils
* Add depends dr-herriot==0.1.0.dev0
* Update depends native-dumper==0.3.7.dev3
* Update depends pgpack-dumper==0.3.7.dev3
* Add Rust core `ddl_core` for generating DDLs from PostgreSQL, Greenplum, and ClickHouse
* Add `generate_ddl()` function returning `ETLInfo` with full table metadata
* Add `TableMetadata` and `ColumnMeta` unified structures for cross-DBMS compatibility
* Add `ConnectionConfig` dataclass for connection configuration
* Add `StagingConfig` dataclass for staging table behavior
* Add `DQConfig` dataclass for Data Quality check configuration
* Add `MoveMethod` enum with `append`, `replace`, `delete`, `rewrite`, `custom` strategies
* Add `DQCheck` enum with `empty`, `uniq`, `future`, `infinity`, `nan`, `total`, `sum` checks
* Add `@etl_pipeline` decorator for automated staging lifecycle management
* Add `DBHose` ETL orchestrator class with `from_dbms`, `from_file`, `from_iterable`, `from_frame` methods
* Add automatic dump format detection (BINARY/CSV) based on source/destination compatibility
* Add PostgreSQL/Greenplum version detection supporting 9.2 through 18+
* Add Greenplum-specific features: `DISTRIBUTED BY`, `USING` access method, `WITH` reloptions
* Add ClickHouse DDL parser with support for `MergeTree`, `ReplicatedMergeTree`, `View`, `MaterializedView`, `Dictionary`
* Add ClickHouse type parsing: `Array`, `Map`, `Tuple`, `Nullable`, `LowCardinality`, `DateTime64`, `FixedString`
* Add ACL parsing and `GRANT` statement generation for PostgreSQL
* Add staging table generation with `UNLOGGED` for PostgreSQL and `MergeTree` for ClickHouse
* Add cross-platform column metadata normalization between PostgreSQL and ClickHouse
* Add comprehensive error hierarchy: `DBHoseError`, `DBHoseValueError`, `DBHoseTypeError`, `DBHoseNotFoundError`, `DBHosePermissionError`
* Add `define_query()` function for loading SQL templates from filesystem
* Add ASCII art logo banner on initialization
* Add `wrap_frame()` utility for visual log framing
* Update `README.md` with complete API documentation and usage examples
* Add `AI_ASSISTANT_CONTRIBUTION.md` documenting collaborative development

## 0.1.0.7

* Fix get metadata text column from postgres/greenplum
* Fix read metadata from readonly postgres/greenplum transactions
* Update depends pgpack-dumper==0.3.5.4
* Update docs directory

## 0.1.0.6

* Update depends native-dumper==0.3.5.3
* Update depends pgpack-dumper==0.3.5.3
* Fix error SQLParseError: Maximum number of tokens exceeded (10000)

## 0.1.0.5

* Update depends native-dumper==0.3.5.2
* Update depends pgpack-dumper==0.3.5.2
* Update docs directory
* Improve DQ check column log output info

## 0.1.0.4

* Update depends native-dumper==0.3.5.1
* Update depends pgpack-dumper==0.3.5.1
* Fix chunk_query function

## 0.1.0.3

* Update README.md
* Update docs directory
* Update depends native-dumper==0.3.5.0
* Update depends pgpack-dumper==0.3.5.0
* Update depends dbhose-utils==0.0.2.5

## 0.1.0.2

* Add depends dbhose-utils==0.0.2.4
* Add documentation link
* Delete OLD_DOCS.md

## 0.1.0.1

* Update depends native-dumper==0.3.4.9
* Update depends pgpack-dumper==0.3.4.8
* Update README.md
* Change DQ output message for check column sum

## 0.1.0.0

* Update depends native-dumper==0.3.4.8
* Update README.md
* Change project status to Beta

## 0.0.4.4

* Update depends pgpack-dumper==0.3.4.7
* Fix pgpack array read function on unix systems
* Fix install on unix systems

## 0.0.4.3

* Update depends pgpack-dumper==0.3.4.6

## 0.0.4.2

* Update depends pgpack-dumper==0.3.4.5
* Fix unpack requires a buffer of 4 bytes for unix systems

## 0.0.4.1

* Update depends pgpack-dumper==0.3.4.4

## 0.0.4.0

* Update depends native-dumper==0.3.4.7
* Update depends pgpack-dumper==0.3.4.3
* Update setuptools to latest version

## 0.0.3.7

* Update depends native-dumper==0.3.4.6
* Update depends pgpack-dumper==0.3.4.1

## 0.0.3.6

* Update depends native-dumper==0.3.4.5

## 0.0.3.5

* Update depends native-dumper==0.3.4.4

## 0.0.3.4

* Update depends native-dumper==0.3.4.3
* Update depends pgpack-dumper==0.3.4.2

## 0.0.3.3

* Update depends native-dumper==0.3.4.2

## 0.0.3.2

* Update depends native-dumper==0.3.4.1

## 0.0.3.1

* Update depends pgpack-dumper==0.3.4.1
* Improve invalid byte sequence for encoding "UTF8": 0x00

## 0.0.3.0

* Update depends native-dumper==0.3.4.0
* Update depends pgpack-dumper==0.3.4.0
* Add auto convert String/FixedString(36) from Clickhouse data to Postgres uuid
* Fix docs show logo

## 0.0.2.8

* Update depends pgpack-dumper==0.3.3.6

## 0.0.2.7

* Fix clickhouse replace partition query
* Improve query_part() function
* Update depends native-dumper==0.3.3.3
* Fix LOGO letters

## 0.0.2.6

* Update depends pgpack-dumper==0.3.3.5
* Refactor from_airflow initialization
* Fix Destination Table diagram

## 0.0.2.5

* Update depends pgpack-dumper==0.3.3.4
* Add docs directory to project

## 0.0.2.4

* Update depends native-dumper==0.3.3.2
* Update depends pgpack-dumper==0.3.3.3
* Add gc collect after write destination table

## 0.0.2.3

* Update depends pgpack-dumper==0.3.3.2
* Fix write_between diagram for Postgres/Greenplum objects

## 0.0.2.2

* Fix include_package_data

## 0.0.2.1

* Add MoveMethod.rewrite for full rewrite table with new data
* Add query_part function
* Change filter_by initialization list to string
* Fix Clickhouse MoveMethod.delete
* Improve execute custom query & MoveMethod operations
* Update depends native-dumper==0.3.3.1
* Update depends pgpack-dumper==0.3.3.1

## 0.0.2.0

* Update depends native-dumper==0.3.3.0
* Update depends pgpack-dumper==0.3.3.0
* Update README.md
* Add create partition into postgres and greenplum ddl queryes
* Improve delete.sql for greenplum and postgres

## 0.0.1.0

* Update depends native-dumper==0.3.2.3
* Update depends pgpack-dumper==0.3.2.2
* Move old README.md into OLD_DOCS.md
* Create new README.md
* Delete dbhose-utils from depends
* Rename repository dbhose -> dbhose_airflow

## 0.0.0.1

First version of the library dbhose_airflow
