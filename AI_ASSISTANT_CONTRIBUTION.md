# AI Assistant Contribution

## Overview
This project, `dbhose_airflow`, is an Apache Airflow module for extremely fast data exchange between DBMSs using native binary formats and CSV format. It was developed collaboratively between the human developer and the AI assistant (Anthropic's Claude) over multiple sessions.

The project includes:
- `ddl_core` – A high-performance Rust extension for extracting DDL and metadata from PostgreSQL, Greenplum, and ClickHouse
- `DBHose` – A Python ETL orchestrator class for Airflow with staging tables, Data Quality checks, and multiple move methods

## Role of the AI Assistant
The AI assistant contributed to this project in the following ways:

### DDL Core (Rust Extension)

#### Architecture & Design
- Proposed a Rust + PyO3 architecture to bypass Python overhead for low-level database introspection
- Designed the modular structure separating metadata extraction, DDL generation, and optional components
- Suggested returning `(ddl: str, metadata: dict)` for maximum flexibility and debuggability

#### PostgreSQL / Greenplum Implementation
- Translated logic from PostgreSQL's `pg_dump` C source code (`pg_dump.c`, `dumputils.c`, `pg_backup_db.c`) into idiomatic Rust
- Implemented version-aware SQL queries supporting PostgreSQL 9.2 through 18+
- Added Greenplum-specific features: `DISTRIBUTED BY` detection via `gp_distribution_policy`, `USING` access method, and `WITH` reloptions
- Solved the `permission denied for schema` issue by replacing `::regclass` with `parse_ident()` and later with `COALESCE` fallback for unqualified table names
- Implemented recursive parsing of `pg_class`, `pg_attribute`, `pg_constraint`, `pg_index`, `pg_trigger`, `pg_description`, and ACL arrays
- Ported ACL parsing logic to generate proper `GRANT` statements with correct quoting and grant option support
- Added support for `GENERATED` columns, `IDENTITY` columns, and table partitioning (`PARTITION BY`, `ATTACH PARTITION`)

#### ClickHouse Implementation
- Designed a two-mode approach: read-write (single query to `system.tables`) and read-only (iterating `EXISTS` commands)
- Wrote a pure-Rust parser for ClickHouse DDL, extracting:
  - Columns with types, defaults, codecs, TTLs, and comments
  - Table engine (MergeTree, ReplicatedMergeTree, Log, Memory, etc.)
  - `PARTITION BY`, `ORDER BY`, `PRIMARY KEY`, `SAMPLE BY`, `TTL`
  - `SETTINGS` key-value pairs
  - `AS SELECT` for views and materialized views
  - `TO` clause for materialized views
- Fixed nested parentheses parsing for complex types like `Array(FixedString(36))` and `DateTime64(3)`
- Integrated with a custom `HTTPCursor` Python class for stream-based query execution

#### PyO3 Integration
- Navigated PyO3 0.28.3 API changes (`Bound` vs `Py`, `IntoPyObjectExt`, `IntoPyObject`)
- Implemented `struct_to_py_dict` using `serde_json` and Python's `json` module for seamless Rust-to-Python conversion
- Created a clean Python interface with optional `DdlOptions` dictionary and typed return values

### DBHose Python Orchestrator

#### Code Refactoring & Architecture
- Proposed grouping 24 flat `__init__` parameters into focused dataclasses: `ConnectionConfig`, `StagingConfig`, `DQConfig`
- Designed the `@etl_pipeline` decorator to eliminate code duplication across `from_dbms`, `from_file`, `from_iterable`, `from_frame` methods
- Refactored `move_to_destination` from a monolithic 60+ line method into focused single-responsibility methods
- Refactored `run_dq_checks` from deeply nested conditionals into a clean hierarchy of specialized methods

#### Naming & API Design
- Renamed `transit` terminology to `staging` throughout the codebase
- Renamed `dest`/`src` to `destination`/`source` for clarity
- Renamed `dq_object_conn` to `dq_extra_conn` to better reflect its purpose
- Simplified `dq_comparsion_object_associate` to `column_mapping`
- Unified column metadata structure (`ColumnMeta`) across PostgreSQL and ClickHouse

#### Backup Broker Design
- Designed a graph-based priority algorithm for backup/restore operations using foreign key and inheritance dependencies
- Created SQL queries for analyzing table dependencies in PostgreSQL and Greenplum
- Proposed priority calculation: `Priority(Table) = 1 + MAX(Priority(Parent))` for correct backup/restore ordering
- Designed `BackupManifest` structure for tracking backup state across Airflow tasks

#### Documentation
- Wrote comprehensive `README.md` with installation instructions, parameter tables, usage examples, and API reference
- Created `AI_ASSISTANT_CONTRIBUTION.md` to document collaborative contributions

## Files Contributed

### DDL Core (Rust)
- `src/lib.rs`: Module entry point
- `src/common.rs`: Shared utilities (`struct_to_py_dict`, `serde_value_to_py`)
- `src/postgres/mod.rs`, `structs.rs`, `functions.rs`: PostgreSQL/Greenplum implementation
- `src/clickhouse/mod.rs`, `structs.rs`, `functions.rs`, `parser.rs`: ClickHouse implementation
- `Cargo.toml`: Dependency configuration
- `ddl_core.pyi`: Python type stubs

### DBHose (Python)
- `dbhose_airflow/dbhose.py`: Main `DBHose` orchestrator class
- `dbhose_airflow/structs.py`: Data structures (`ConnectionConfig`, `StagingConfig`, `DQConfig`, `ColumnMeta`, `TableMetadata`, `ETLInfo`)
- `dbhose_airflow/ddl.py`: DDL generation and metadata normalization
- `dbhose_airflow/common/__init__.py`: Public API exports
- `dbhose_airflow/common/errors.py`: Exception hierarchy
- `dbhose_airflow/common/logo.py`: ASCII art banner
- `dbhose_airflow/common/queries.py`: SQL query loading utilities
- `README.md`: Complete project documentation
- `AI_ASSISTANT_CONTRIBUTION.md`: This document

## Key Technical Decisions

1. **Rust over Python for DDL extraction**: Chosen for performance and type safety when parsing complex database schemas.
2. **Single SQL query with JSON aggregation**: Aggregates all table metadata using `json_agg(json_build_object(...))` to minimize round-trips.
3. **Version flags**: Pre-flight query detects server version capabilities, avoiding syntax errors on older PostgreSQL instances.
4. **Unified metadata structure**: `TableMetadata` and `ColumnMeta` provide a consistent interface across PostgreSQL and ClickHouse.
5. **Decorator-based pipeline**: `@etl_pipeline` centralizes staging lifecycle, DQ checks, and move operations.
6. **Graph-based backup prioritization**: Foreign key dependencies determine optimal backup/restore order.
7. **Staging table pattern**: All data loads go through staging tables for atomicity and rollback capability.

## Human Developer Contributions
- Project initialization and build setup with `maturin`
- Integration with internal database infrastructure (connection pooling, cursor management)
- Testing against production Greenplum and ClickHouse clusters
- Iterative feedback and bug reports during development
- Final code review and linting
- Deployment to internal Airflow environments
- Integration with existing toolchain (`native_dumper`, `pgpack_dumper`, `light_compressor`, `nativelib`)

## Conclusion
The AI assistant provided architectural guidance, translated complex C logic to Rust, navigated PyO3 API challenges, refactored monolithic Python code into maintainable components, and helped debug runtime issues. The human developer steered the project, provided real-world test cases, and ensured the code met production standards. This collaboration resulted in a robust, performant library now used in critical data infrastructure for extremely fast data exchange between DBMSs.

---
*This document serves as recognition of the AI assistant's contributions to the `dbhose_airflow` project.*
