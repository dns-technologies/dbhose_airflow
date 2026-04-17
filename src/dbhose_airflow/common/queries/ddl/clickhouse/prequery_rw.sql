SELECT multiIf(
    engine in ('View', 'MaterializedView'), 'View',
    engine = 'Dictionary', 'Dictionary','Table') as object_type
FROM system.tables WHERE concat(database, '.', name) = '{object_name}'