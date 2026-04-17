SELECT 
    c.oid,
    c.relname,
    c.relowner,
    c.relkind,
    c.relpersistence,
    c.reloptions,
    c.reltablespace,
    c.relacl,
    n.nspname AS schema_name,
    pg_catalog.pg_get_userbyid(c.relowner) AS owner_name,
    (SELECT json_agg(json_build_object(
        'attnum', a.attnum, 'attname', a.attname, 'atttypid', a.atttypid, 
        'attnotnull', a.attnotnull, 'atthasdef', a.atthasdef, 
        'attoptions', a.attoptions, 'attfdwoptions', a.attfdwoptions,
        'typname', t.typname, 'typnamespace', t.typnamespace, 
        'defaultval', pg_catalog.pg_get_expr(d.adbin, d.adrelid)
    )) FROM pg_catalog.pg_attribute a
    LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
    LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
    WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
    ) AS columns,
    (SELECT json_agg(json_build_object(
        'conname', conname, 'contype', contype, 'condef', pg_catalog.pg_get_constraintdef(oid, true)
    )) FROM pg_catalog.pg_constraint WHERE conrelid = c.oid) AS constraints
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE c.oid = '{object_name}'::regclass;