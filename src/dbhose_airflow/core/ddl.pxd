cdef object PRIMARY_KEY
cdef dict SERVER_NAME

cdef object _normalize_postgres_meta(dict meta)
cdef object _normalize_clickhouse_meta(dict meta)
cdef object _find_column_comment(list comments, int attnum)
cdef object _pg_generated(str value)
cdef object _pg_identity(str value)
cdef object _parse_reloptions(list options)

cdef object normalize_metadata(dict table_meta, bint is_postgres)
cdef list __validate_ddl(dict table_meta)
cdef str __build_clickhouse_staging_ddl_full(
    str staging_table,
    dict table_meta,
)
cdef str __build_clickhouse_staging_ddl_simple(
    str staging_table,
    dict table_meta,
)
cdef str __build_postgres_staging_ddl_full(
    str staging_table,
    dict table_meta,
)
cdef str __build_postgres_staging_ddl_simple(
    str staging_table,
    dict table_meta,
)
cdef tuple build_staging_ddls(
    str staging_table,
    dict table_meta,
    bint is_postgres,
)
