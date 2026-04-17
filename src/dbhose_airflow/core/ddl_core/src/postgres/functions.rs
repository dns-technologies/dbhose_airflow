use pyo3::prelude::*;
use pyo3::types::{
    PyAny,
    PyDict,
    PyList,
};

use crate::common::struct_to_py_dict;
use super::structs::{
    AclItem,
    ColumnInfo,
    ConstraintInfo,
    DdlOptions,
    IndexInfo,
    ParentTable,
    PartitionInfo,
    SequenceMetadata,
    TableComment,
    TableMetadata,
    TriggerInfo,
};


fn parse_acl(acl_str: &str) -> Vec<AclItem> {
    let mut items = Vec::new();
    let acl_str = acl_str.trim_matches(|c| c == '{' || c == '}');

    if acl_str.is_empty() {
        return items;
    }

    for item in acl_str.split(',') {
        let parts: Vec<&str> = item.split('/').collect();

        if parts.len() != 2 {
            continue;
        }

        let grantor = clean_identifier(parts[1].trim());
        let grantee_parts: Vec<&str> = parts[0].split('=').collect();

        if grantee_parts.len() != 2 {
            continue;
        }

        let grantee = if grantee_parts[0].is_empty() {
            "PUBLIC".to_string()
        } else {
            clean_identifier(grantee_parts[0])
        };
        let priv_str = grantee_parts[1];
        let with_grant_option = priv_str.ends_with('*');
        let priv_str = priv_str.trim_end_matches('*');
        let privileges: Vec<String> = priv_str.chars().map(|c| match c {
            'r' => "SELECT".to_string(),
            'w' => "UPDATE".to_string(),
            'a' => "INSERT".to_string(),
            'd' => "DELETE".to_string(),
            'D' => "TRUNCATE".to_string(),
            'x' => "REFERENCES".to_string(),
            't' => "TRIGGER".to_string(),
            'U' => "USAGE".to_string(),
            'C' => "CREATE".to_string(),
            'c' => "CONNECT".to_string(),
            'T' => "TEMPORARY".to_string(),
            'X' => "EXECUTE".to_string(),
            _ => String::new(),
        }).filter(|s| !s.is_empty()).collect();
        items.push(AclItem {
            grantee,
            grantor,
            privileges,
            with_grant_option,
        });
    }

    items
}


fn clean_identifier(s: &str) -> String {
    let s = s.trim();
    let s = if s.starts_with('"') && s.ends_with('"') {
        &s[1..s.len()-1]
    } else {
        s
    };
    s.replace('\\', "").replace('"', "")
}


#[pyfunction]
pub fn postgres_sequence_ddl(
    py: Python<'_>, 
    cursor: Py<PyAny>, 
    object_name: String
) -> PyResult<(String, Py<PyDict>)> {
    let cursor_bound = cursor.bind(py);
    let server_query = r#"
        SELECT
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM pg_catalog.pg_namespace
                    WHERE nspname = 'gp_toolkit'
                )
                THEN 'greenplum'
                ELSE 'postgres'
            END AS server_type
    "#;
    cursor_bound.call_method1("execute", (server_query,))?;
    let query = format!(
        r#"
        WITH c_rel AS (
            SELECT c.oid 
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = (pg_catalog.parse_ident('{}'))[1]
            AND c.relname = (pg_catalog.parse_ident('{}'))[2]
        )
        SELECT 
            c.relname AS seqname,
            n.nspname AS seqschema,
            pg_catalog.pg_get_userbyid(c.relowner) AS owner,
            c.relacl::text AS relacl,
            pg_catalog.pg_get_serial_sequence(
                (SELECT nspname || '.' || relname FROM pg_catalog.pg_class WHERE oid = (SELECT oid FROM c_rel))
            ) AS owned_by,
            format('CREATE SEQUENCE %I.%I', n.nspname, c.relname) || 
            COALESCE(' START ' || s.start_value, '') ||
            COALESCE(' INCREMENT ' || s.increment_by, '') ||
            COALESCE(' MINVALUE ' || s.min_value, ' NO MINVALUE') ||
            COALESCE(' MAXVALUE ' || s.max_value, ' NO MAXVALUE') ||
            COALESCE(' CACHE ' || s.cache_value, '') ||
            CASE WHEN s.is_cycled THEN ' CYCLE' ELSE '' END AS seqdef
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_catalog.pg_sequence s ON c.oid = s.seqrelid
        WHERE c.oid = (SELECT oid FROM c_rel) AND c.relkind = 'S'
        "#,
        object_name.replace("'", "''"),
        object_name.replace("'", "''")
    );

    cursor_bound.call_method1("execute", (query.as_str(),))?;
    let row = cursor_bound.call_method0("fetchone")?;

    if row.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Sequence '{}' not found", object_name)
        ));
    }

    let meta = SequenceMetadata {
        seqname: row.get_item(0)?.extract()?,
        seqschema: row.get_item(1)?.extract()?,
        owner: row.get_item(2)?.extract()?,
        relacl: row.get_item(3)?.extract::<Option<String>>()?,
        owned_by: row.get_item(4)?.extract::<Option<String>>()?,
        seqdef: row.get_item(5)?.extract()?,
    };
    let opts = DdlOptions::default();
    let ddl = generate_sequence_ddl(&meta, &opts);
    let metadata_dict = PyDict::new(py);
    metadata_dict.set_item("seqname", meta.seqname)?;
    metadata_dict.set_item("seqschema", meta.seqschema)?;
    metadata_dict.set_item("owner", meta.owner)?;
    metadata_dict.set_item("owned_by", meta.owned_by)?;
    metadata_dict.set_item("seqdef", meta.seqdef)?;
    Ok((ddl, metadata_dict.into()))
}


fn generate_sequence_ddl(meta: &SequenceMetadata, opts: &DdlOptions) -> String {
    let mut ddl = String::new();
    ddl.push_str(&meta.seqdef);
    ddl.push_str(";\n");

    if opts.include_owner {
        ddl.push_str(&format!(
            "\nALTER SEQUENCE {}.{} OWNER TO {};\n",
            quote_identifier(&meta.seqschema),
            quote_identifier(&meta.seqname),
            quote_identifier(&meta.owner)
        ));
    }

    if let Some(ref owned_by) = meta.owned_by {
        if !owned_by.is_empty() {
            ddl.push_str(&format!(
                "\nALTER SEQUENCE {}.{} OWNED BY {};\n",
                quote_identifier(&meta.seqschema),
                quote_identifier(&meta.seqname),
                owned_by
            ));
        }
    }

    if opts.include_acl {
        if let Some(acl) = &meta.relacl {
            if acl != "null" && !acl.is_empty() {
                let acl_items = parse_acl(acl);
                let full_name = format!(
                    "{}.{}",
                    quote_identifier(&meta.seqschema),
                    quote_identifier(&meta.seqname)
                );

                for item in acl_items {
                    let privs = item.privileges.join(", ");
                    if !privs.is_empty() {
                        let grantee = if item.grantee == "PUBLIC" {
                            "PUBLIC".to_string()
                        } else if needs_quoting(&item.grantee) {
                            quote_identifier(&item.grantee)
                        } else {
                            item.grantee.clone()
                        };
                        ddl.push_str(&format!(
                            "\nGRANT {} ON SEQUENCE {} TO {}",
                            privs, full_name, grantee
                        ));
                        if item.with_grant_option {
                            ddl.push_str(" WITH GRANT OPTION");
                        }
                        ddl.push_str(";\n");
                    }
                }
            }
        }
    }

    ddl
}


fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace("\"", "\"\""))
}


fn generate_table_ddl(meta: &TableMetadata, opts: &DdlOptions) -> String {
    let mut ddl = String::new();

    if meta.relkind == "v" || meta.relkind == "m" {
        ddl.push_str(&generate_view_base(meta));
    } else {
        ddl.push_str(&generate_table_base(meta, opts));

        if meta.partition.is_partitioned && opts.include_partitions {
            if let Some(ref key) = meta.partition.partition_key {
                let pos = ddl.rfind(");").unwrap_or(ddl.len());
                ddl.insert_str(pos, &format!("\nPARTITION BY {}", key));
            }
        }

        ddl.push_str(";\n");

        if meta.partition.is_partition && opts.include_partitions {
            ddl.push_str(&generate_attach_partition(meta));
        }

        ddl.push_str(&generate_indexes(meta, opts));
        ddl.push_str(&generate_constraints(meta, opts));
    }

    ddl.push_str(&generate_owner(meta, opts));
    ddl.push_str(&generate_comments(meta, opts));
    ddl.push_str(&generate_acl(meta, opts));
    ddl.push_str(&generate_triggers(meta, opts));
    ddl
}


fn generate_view_base(meta: &TableMetadata) -> String {
    let mut ddl = String::new();
    let view_type = if meta.relkind == "m" { "MATERIALIZED VIEW" } else { "VIEW" };
    ddl.push_str(&format!(
        "CREATE {} {}.{}",
        view_type,
        quote_identifier(&meta.schema_name),
        quote_identifier(&meta.relname)
    ));

    if !meta.columns.is_empty() {
        let col_names: Vec<String> = meta.columns.iter()
            .map(|c| quote_identifier(&c.attname))
            .collect();
        ddl.push_str(&format!(" ({})", col_names.join(", ")));
    }

    ddl.push_str(&format!(" AS\n{}", meta.view_definition.as_ref().unwrap_or(&String::new())));

    if meta.relkind == "m" {
        ddl.push_str(if meta.with_data { "\nWITH DATA;" } else { "\nWITH NO DATA;" });
    } else {
        ddl.push_str(";");
    }
    ddl.push('\n');

    ddl
}


fn generate_table_base(meta: &TableMetadata, opts: &DdlOptions) -> String {
    let mut ddl = String::new();
    ddl.push_str(&format!(
        "CREATE {}{}TABLE {}.{} (\n",
        if meta.relpersistence == "u" { "UNLOGGED " } else { "" },
        if meta.relkind == "f" { "FOREIGN " } else { "" },
        quote_identifier(&meta.schema_name),
        quote_identifier(&meta.relname)
    ));
    let col_defs: Vec<String> = meta.columns.iter()
        .map(|col| generate_column_definition(col))
        .collect();
    ddl.push_str(&col_defs.join(",\n"));
    let table_constraints: Vec<String> = meta.constraints.iter()
        .filter(|c| c.contype == "p" || c.contype == "u")
        .map(|c| format!("    CONSTRAINT {} {}", quote_identifier(&c.conname), c.condef))
        .collect();

    if !table_constraints.is_empty() {
        ddl.push_str(",\n");
        ddl.push_str(&table_constraints.join(",\n"));
    }

    ddl.push_str("\n)");

    if let Some(ref am) = meta.access_method {
        if !am.is_empty() && am != "heap" {
            ddl.push_str(&format!("\nUSING {}", am.to_uppercase()));
        }
    }

    if let Some(ref relopts) = meta.reloptions {
        if !relopts.is_empty() {
            ddl.push_str(&format!("\nWITH (\n\t{}\n)", relopts.join(",\n\t")));
        }
    }

    if opts.include_distributed_by {
        if let Some(ref cols) = meta.distkey {
            if !cols.is_empty() {
                let quoted_cols: Vec<String> = cols.iter()
                    .map(|c| quote_identifier(c))
                    .collect();
                ddl.push_str(&format!("\nDISTRIBUTED BY ({})", quoted_cols.join(", ")));
            } else {
                ddl.push_str("\nDISTRIBUTED RANDOMLY");
            }
        }
    }

    ddl.push_str(";\n");
    ddl
}


fn generate_column_definition(col: &ColumnInfo) -> String {
    let mut def = format!("    {} {}", quote_identifier(&col.attname), col.typname);

    if col.attidentity == "a" {
        def.push_str(" GENERATED ALWAYS AS IDENTITY");
    } else if col.attidentity == "d" {
        def.push_str(" GENERATED BY DEFAULT AS IDENTITY");
    }

    if col.attgenerated == "s" {
        def.push_str(" GENERATED ALWAYS AS (???) STORED");
    }

    if col.attnotnull {
        def.push_str(" NOT NULL");
    }

    if let Some(default) = &col.defaultval {
        if col.attidentity.is_empty() && col.attgenerated.is_empty() {
            def.push_str(&format!(" DEFAULT {}", default));
        }
    }

    def
}


fn generate_attach_partition(meta: &TableMetadata) -> String {
    let mut ddl = String::new();

    for parent in &meta.partition.parents {
        if let Some(ref bound) = meta.partition.partition_bound {
            ddl.push_str(&format!(
                "\nALTER TABLE {}.{} ATTACH PARTITION {}.{} {};\n",
                quote_identifier(&parent.parent_schema),
                quote_identifier(&parent.parent_table),
                quote_identifier(&meta.schema_name),
                quote_identifier(&meta.relname),
                bound
            ));
        }
    }
    
    ddl
}


fn generate_indexes(meta: &TableMetadata, opts: &DdlOptions) -> String {
    let mut ddl = String::new();

    if !opts.include_indexes {
        return ddl;
    }

    for idx in &meta.indexes {
        if idx.indexconstraint.is_some() || idx.parentidx != 0 {
            continue;
        }

        ddl.push_str(&format!("\n{};\n", idx.indexdef));

        if idx.indisclustered {
            ddl.push_str(&format!(
                "\nALTER TABLE {}.{} CLUSTER ON \"{}\";\n",
                quote_identifier(&meta.schema_name),
                quote_identifier(&meta.relname),
                idx.indexname
            ));
        }

        if idx.indisreplident {
            ddl.push_str(&format!(
                "\nALTER TABLE {}.{} REPLICA IDENTITY USING INDEX \"{}\";\n",
                quote_identifier(&meta.schema_name),
                quote_identifier(&meta.relname),
                idx.indexname
            ));
        }
    }

    ddl
}


fn generate_constraints(meta: &TableMetadata, opts: &DdlOptions) -> String {
    let mut ddl = String::new();
    let table_name = format!(
        "{}.{}",
        quote_identifier(&meta.schema_name),
        quote_identifier(&meta.relname)
    );

    if opts.include_constraints_fk {
        for con in meta.constraints.iter().filter(|c| c.contype == "f") {
            ddl.push_str(&format!(
                "\nALTER TABLE {} ADD CONSTRAINT {} {};\n",
                table_name,
                quote_identifier(&con.conname),
                con.condef
            ));
        }
    }

    if opts.include_constraints_check {
        for con in meta.constraints.iter().filter(|c| c.contype == "c") {
            ddl.push_str(&format!(
                "\nALTER TABLE {} ADD CONSTRAINT {} {};\n",
                table_name,
                quote_identifier(&con.conname),
                con.condef
            ));
        }
    }

    ddl
}


fn generate_owner(meta: &TableMetadata, opts: &DdlOptions) -> String {
    if !opts.include_owner {
        return String::new();
    }

    let obj_type = match meta.relkind.as_str() {
        "v" => "VIEW",
        "m" => "MATERIALIZED VIEW",
        "S" => "SEQUENCE",
        _ => "TABLE",
    };
    format!(
        "\nALTER {} {}.{} OWNER TO {};\n",
        obj_type,
        quote_identifier(&meta.schema_name),
        quote_identifier(&meta.relname),
        quote_identifier(&meta.owner_name)
    )
}


fn generate_comments(meta: &TableMetadata, opts: &DdlOptions) -> String {
    if !opts.include_comments {
        return String::new();
    }

    let mut ddl = String::new();
    let obj_type = match meta.relkind.as_str() {
        "v" => "VIEW",
        "m" => "MATERIALIZED VIEW",
        _ => "TABLE",
    };
    let full_name = format!(
        "{}.{}",
        quote_identifier(&meta.schema_name),
        quote_identifier(&meta.relname)
    );

    for comment in &meta.comments {
        if comment.objsubid == 0 {
            ddl.push_str(&format!(
                "\nCOMMENT ON {} {} IS {};\n",
                obj_type,
                full_name,
                quote_literal(&comment.description)
            ));
        }
    }

    for comment in &meta.comments {
        if comment.objsubid > 0 {
            if let Some(col) = meta.columns.get((comment.objsubid - 1) as usize) {
                ddl.push_str(&format!(
                    "\nCOMMENT ON COLUMN {}.{} IS {};\n",
                    full_name,
                    quote_identifier(&col.attname),
                    quote_literal(&comment.description)
                ));
            }
        }
    }
    
    ddl
}


fn generate_acl(meta: &TableMetadata, opts: &DdlOptions) -> String {
    if !opts.include_acl {
        return String::new();
    }

    let mut ddl = String::new();

    if let Some(acl) = &meta.relacl {
        if acl == "null" || acl.is_empty() {
            return ddl;
        }

        let obj_type = match meta.relkind.as_str() {
            "v" => "VIEW",
            "m" => "MATERIALIZED VIEW",
            "S" => "SEQUENCE",
            _ => "TABLE",
        };
        let full_name = format!(
            "{}.{}",
            quote_identifier(&meta.schema_name),
            quote_identifier(&meta.relname)
        );
        let acl_items = parse_acl(acl);

        for item in acl_items {
            let privs = item.privileges.join(", ");

            if !privs.is_empty() {
                let grantee = if item.grantee == "PUBLIC" {
                    "PUBLIC".to_string()
                } else if needs_quoting(&item.grantee) {
                    quote_identifier(&item.grantee)
                } else {
                    item.grantee.clone()
                };
                ddl.push_str(&format!(
                    "\nGRANT {} ON {} {} TO {}",
                    privs, obj_type, full_name, grantee
                ));

                if item.with_grant_option {
                    ddl.push_str(" WITH GRANT OPTION");
                }

                ddl.push_str(";\n");
            }
        }
    }
    
    ddl
}


fn generate_triggers(meta: &TableMetadata, opts: &DdlOptions) -> String {
    if !opts.include_triggers {
        return String::new();
    }

    let mut ddl = String::new();
    let table_name = format!(
        "{}.{}",
        quote_identifier(&meta.schema_name),
        quote_identifier(&meta.relname)
    );

    for trig in &meta.triggers {
        ddl.push_str(&format!("\n{};\n", trig.tgdef));

        if trig.tgenabled != "O" {
            let action = match trig.tgenabled.as_str() {
                "D" | "f" => "DISABLE",
                "A" => "ENABLE ALWAYS",
                "R" => "ENABLE REPLICA",
                _ => "ENABLE",
            };
            ddl.push_str(&format!(
                "\nALTER TABLE {} {} TRIGGER {};\n",
                table_name,
                action,
                quote_identifier(&trig.tgname)
            ));
        }
    }
    
    ddl
}


fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace("'", "''"))
}


fn get_bool_opt(dict: &Bound<'_, PyDict>, key: &str, default: bool) -> PyResult<bool> {
    match dict.get_item(key)? {
        Some(v) if !v.is_none() => v.extract(),
        _ => Ok(default),
    }
}


fn needs_quoting(s: &str) -> bool {
    s.chars().any(|c| {
        c.is_uppercase() || 
        c == '.' || 
        c == '-' || 
        c == '@' || 
        c == ' ' ||
        !c.is_ascii_lowercase()
    }) || s.parse::<i64>().is_ok()
}


#[pyfunction]
#[pyo3(signature = (cursor, object_name, options=None))]
pub fn postgres_ddl(
    py: Python<'_>, 
    cursor: Py<PyAny>, 
    object_name: String,
    options: Option<Py<PyDict>>,
) -> PyResult<(String, Py<PyDict>)> {
    let opts = if let Some(dict) = options {
        let dict = dict.bind(py).cast::<PyDict>()?;
        DdlOptions {
            include_indexes: get_bool_opt(dict, "include_indexes", true)?,
            include_constraints_fk: get_bool_opt(dict, "include_constraints_fk", true)?,
            include_constraints_check: get_bool_opt(dict, "include_constraints_check", true)?,
            include_owner: get_bool_opt(dict, "include_owner", true)?,
            include_comments: get_bool_opt(dict, "include_comments", true)?,
            include_acl: get_bool_opt(dict, "include_acl", true)?,
            include_distributed_by: get_bool_opt(dict, "include_distributed_by", true)?,
            include_partitions: get_bool_opt(dict, "include_partitions", true)?,
            include_triggers: get_bool_opt(dict, "include_triggers", true)?,
        }
    } else {
        DdlOptions::default()
    };
    let version_query = r#"
        SELECT
            CASE WHEN current_setting('server_version_num')::int >= 100000
                 THEN true ELSE false END AS has_identity,
            CASE WHEN current_setting('server_version_num')::int >= 100000
                 THEN true ELSE false END AS has_partitioning,
            CASE WHEN current_setting('server_version_num')::int >= 110000
                 THEN true ELSE false END AS has_generated,
            CASE WHEN current_setting('server_version_num')::int >= 120000
                 THEN true ELSE false END AS has_missingval,
            CASE WHEN current_setting('server_version_num')::int >= 140000
                 THEN true ELSE false END AS has_compression,
            CASE WHEN current_setting('server_version_num')::int >= 150000
                 THEN true ELSE false END AS has_nulls_not_distinct,
            CASE WHEN current_setting('server_version_num')::int >= 170000
                 THEN true ELSE false END AS has_conperiod,
            CASE WHEN current_setting('server_version_num')::int >= 180000
                 THEN true ELSE false END AS has_relallfrozen,
            CASE WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = 'gp_toolkit')
                 THEN 'greenplum' ELSE 'postgres' END AS server_type
    "#;
    let cursor_bound = cursor.bind(py);
    cursor_bound.call_method1("execute", (version_query,))?;
    let version_row = cursor_bound.call_method0("fetchone")?;
    let has_identity: bool = version_row.get_item(0)?.extract()?;
    let has_partitioning: bool = version_row.get_item(1)?.extract()?;
    let has_generated: bool = version_row.get_item(2)?.extract()?;
    let has_missingval: bool = version_row.get_item(3)?.extract()?;
    let has_compression: bool = version_row.get_item(4)?.extract()?;
    let has_nulls_not_distinct: bool = version_row.get_item(5)?.extract()?;
    let has_conperiod: bool = version_row.get_item(6)?.extract()?;
    let has_relallfrozen: bool = version_row.get_item(7)?.extract()?;
    let server_type: String = version_row.get_item(8)?.extract()?;
    let query = format!(
        r#"
        WITH c_rel AS (
        SELECT c.oid
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = COALESCE((pg_catalog.parse_ident('{}'))[1], 'public')
            AND c.relname = COALESCE((pg_catalog.parse_ident('{}'))[2],
            (pg_catalog.parse_ident('{}'))[1])
        )
        SELECT
            c.oid::int4,
            c.relname::varchar,
            c.relowner::int4,
            c.relkind::varchar,
            c.relpersistence::varchar,
            c.reloptions::text[] AS reloptions,
            c.reltablespace::int4,
            c.relacl::varchar,
            {} AS relallfrozen,
            n.nspname::varchar AS schema_name,
            pg_catalog.pg_get_userbyid(c.relowner)::varchar AS owner_name,
            {} AS ispartition,
            {} AS partkeydef,
            {} AS partbound,
            (SELECT json_agg(json_build_object(
                'attnum', a.attnum,
                'attname', a.attname,
                'atttypid', a.atttypid,
                'attnotnull', a.attnotnull,
                'atthasdef', a.atthasdef,
                'attoptions', a.attoptions::text,
                'attfdwoptions', a.attfdwoptions::text,
                'typname', t.typname,
                'typnamespace', t.typnamespace,
                'defaultval', pg_catalog.pg_get_expr(d.adbin, d.adrelid),
                'attidentity', {},
                'attgenerated', {},
                'attcompression', {},
                'attmissingval', {}
            )) FROM pg_catalog.pg_attribute a
            LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
            LEFT JOIN pg_catalog.pg_attrdef d
                ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
            WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
            ) AS columns,
            (SELECT json_agg(json_build_object(
                'conname', conname,
                'contype', contype,
                'condef', pg_catalog.pg_get_constraintdef(oid, true),
                'conperiod', {}
            )) FROM pg_catalog.pg_constraint WHERE conrelid = c.oid
            ) AS constraints,
            (SELECT json_agg(json_build_object(
                'indexname', i.indexname,
                'indexdef', i.indexdef,
                'tablespace', i.tablespace,
                'indreloptions', i.indreloptions,
                'indisclustered', i.indisclustered,
                'indisreplident', i.indisreplident,
                'indnullsnotdistinct', i.indnullsnotdistinct,
                'parentidx', i.parentidx,
                'indnkeyattrs', i.indnkeyattrs,
                'indexconstraint', i.indexconstraint
            )) FROM (
                SELECT
                    t.relname AS indexname,
                    pg_catalog.pg_get_indexdef(i.indexrelid) AS indexdef,
                    (SELECT spcname FROM pg_catalog.pg_tablespace s
                    WHERE s.oid = t.reltablespace) AS tablespace,
                    array_to_string(t.reloptions, ',') AS indreloptions,
                    i.indisclustered,
                    i.indisreplident,
                    {} AS indnullsnotdistinct,
                    {} AS parentidx,
                    {} AS indnkeyattrs,
                    c_idx.oid AS indexconstraint
                FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_class t ON t.oid = i.indexrelid
                LEFT JOIN pg_catalog.pg_inherits inh
                    ON inh.inhrelid = i.indexrelid
                LEFT JOIN pg_catalog.pg_constraint c_idx
                    ON c_idx.conindid = i.indexrelid
                    AND c_idx.conrelid = i.indrelid
                WHERE i.indrelid = (SELECT oid FROM c_rel)
                    AND i.indisvalid
                    AND i.indisready
                    AND t.relkind IN ('i', 'I')
            ) i) AS indexes,
            (SELECT json_agg(json_build_object(
                'objsubid', d.objsubid,
                'description', d.description::text
            )) FROM pg_catalog.pg_description d
            WHERE d.objoid = c.oid
            AND d.classoid = 'pg_catalog.pg_class'::regclass) AS comments,
            {} AS distkey,
            (SELECT pg_catalog.pg_get_viewdef(c.oid, true)) AS view_definition,
            c.relispopulated AS with_data,
            (SELECT json_agg(json_build_object(
                'parent_schema', pn.nspname,
                'parent_table', pc.relname
            )) FROM pg_catalog.pg_inherits i
            JOIN pg_catalog.pg_class pc ON i.inhparent = pc.oid
            JOIN pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
            WHERE i.inhrelid = c.oid) AS parents,
            (SELECT json_agg(json_build_object(
                'tgname', t.tgname,
                'tgdef', pg_catalog.pg_get_triggerdef(t.oid, true),
                'tgenabled', t.tgenabled,
                'tgispartition', t.tgisinternal
            )) FROM pg_catalog.pg_trigger t
            WHERE t.tgrelid = c.oid
            AND NOT t.tgisinternal) AS triggers,
            am.amname AS access_method
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
        LEFT JOIN pg_catalog.pg_am am ON c.relam = am.oid
        WHERE c.oid = (SELECT oid FROM c_rel)
        "#,
        object_name.replace("'", "''"),
        object_name.replace("'", "''"),
        object_name.replace("'", "''"),
        if has_relallfrozen { "c.relallfrozen" } else { "0" },
        if has_partitioning { "c.relispartition" } else { "false" },
        if has_partitioning { "pg_catalog.pg_get_partkeydef(c.oid)" } else { "NULL" },
        if has_partitioning { "pg_catalog.pg_get_expr(c.relpartbound, c.oid)" } else { "NULL" },
        if has_identity { "a.attidentity" } else { "''" },
        if has_generated { "a.attgenerated" } else { "''" },
        if has_compression { "a.attcompression" } else { "''" },
        if has_missingval { "a.attmissingval::text" } else { "NULL" },
        if has_conperiod { "c.conperiod" } else { "false" },
        if has_nulls_not_distinct { "i.indnullsnotdistinct" } else { "false" },
        if has_partitioning { "COALESCE(inh.inhparent, 0)" } else { "0" },
        if has_partitioning { "i.indnkeyatts" } else { "i.indnatts" },
        if server_type == "greenplum" {
            r#"
            (SELECT array_agg(a.attname ORDER BY array_position(p.distkey, a.attnum))
            FROM pg_catalog.gp_distribution_policy p
            JOIN pg_catalog.pg_attribute a ON a.attrelid = p.localoid 
            WHERE p.localoid = c.oid AND a.attnum = ANY(p.distkey))
            "#
        } else {
            "NULL"
        }
    );
    cursor_bound.call_method1("execute", (query.as_str(),))?;
    let row = cursor_bound.call_method0("fetchone")?;

    if row.is_none() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("Object '{}' not found", object_name)
        ));
    }

    let ispartition_val = row.get_item(11)?;
    let is_partition: bool = if ispartition_val.is_none() {
        false
    } else {
        ispartition_val.extract()?
    };

    let partkeydef_val = row.get_item(12)?;
    let partkeydef: Option<String> = if partkeydef_val.is_none() {
        None
    } else {
        Some(partkeydef_val.extract()?)
    };

    let partbound_val = row.get_item(13)?;
    let partbound: Option<String> = if partbound_val.is_none() {
        None
    } else {
        Some(partbound_val.extract()?)
    };
    let parents_val = row.get_item(21)?;
    let parents: Vec<ParentTable> = if parents_val.is_none() {
        Vec::new()
    } else {
        let list = parents_val.cast::<PyList>()?;
        list.iter()
            .map(|item| {
                let dict = item.cast::<PyDict>()?;
                Ok(ParentTable {
                    parent_schema: dict.get_item("parent_schema")?.unwrap().extract()?,
                    parent_table: dict.get_item("parent_table")?.unwrap().extract()?,
                })
            })
            .collect::<PyResult<Vec<_>>>()?
    };
    let partition = PartitionInfo {
        is_partitioned: !is_partition && partkeydef.is_some(),
        is_partition,
        partition_bound: partbound,
        partition_key: partkeydef,
        parents,
    };
    let columns_val = row.get_item(14)?;
    let columns: Vec<ColumnInfo> = if columns_val.is_none() {
        Vec::new()
    } else {
        let list = columns_val.cast::<PyList>()?;
        list.iter()
            .map(|item| {
                let dict = item.cast::<PyDict>()?;
                Ok(ColumnInfo {
                    attnum: dict.get_item("attnum")?.unwrap().extract()?,
                    attname: dict.get_item("attname")?.unwrap().extract()?,
                    atttypid: {
                        let v = dict.get_item("atttypid")?.unwrap();
                        if let Ok(n) = v.extract::<u32>() {
                            n
                        } else {
                            v.extract::<String>()?.parse::<u32>().unwrap_or(0)
                        }
                    },
                    attnotnull: dict.get_item("attnotnull")?.unwrap().extract()?,
                    atthasdef: dict.get_item("atthasdef")?.unwrap().extract()?,
                    attoptions: match dict.get_item("attoptions")? {
                        Some(v) if !v.is_none() => Some(v.extract()?),
                        _ => None,
                    },
                    attfdwoptions: match dict.get_item("attfdwoptions")? {
                        Some(v) if !v.is_none() => Some(v.extract()?),
                        _ => None,
                    },
                    typname: dict.get_item("typname")?.unwrap().extract()?,
                    typnamespace: {
                        let v = dict.get_item("typnamespace")?.unwrap();
                        if let Ok(n) = v.extract::<u32>() {
                            n
                        } else {
                            v.extract::<String>()?.parse::<u32>().unwrap_or(0)
                        }
                    },
                    defaultval: match dict.get_item("defaultval")? {
                        Some(v) if !v.is_none() => Some(v.extract()?),
                        _ => None,
                    },
                    attidentity: dict.get_item("attidentity")?.unwrap().extract()?,
                    attgenerated: dict.get_item("attgenerated")?.unwrap().extract()?,
                    attcompression: dict.get_item("attcompression")?.unwrap().extract()?,
                    attmissingval: match dict.get_item("attmissingval")? {
                        Some(v) if !v.is_none() => Some(v.extract()?),
                        _ => None,
                    },
                })
            })
            .collect::<PyResult<Vec<_>>>()?
    };
    let constraints_val = row.get_item(15)?;
    let constraints: Vec<ConstraintInfo> = if constraints_val.is_none() {
        Vec::new()
    } else {
        let list = constraints_val.cast::<PyList>()?;
        list.iter()
            .map(|item| {
                let dict = item.cast::<PyDict>()?;
                Ok(ConstraintInfo {
                    conname: dict.get_item("conname")?.unwrap().extract()?,
                    contype: dict.get_item("contype")?.unwrap().extract()?,
                    condef: dict.get_item("condef")?.unwrap().extract()?,
                    conperiod: dict.get_item("conperiod")?.unwrap().extract()?,
                })
            })
            .collect::<PyResult<Vec<_>>>()?
    };
    let indexes_val = row.get_item(16)?;
    let indexes: Vec<IndexInfo> = if indexes_val.is_none() {
        Vec::new()
    } else {
        let list = indexes_val.cast::<PyList>()?;
        list.iter()
            .map(|item| {
                let dict = item.cast::<PyDict>()?;
                Ok(IndexInfo {
                    indexname: dict.get_item("indexname")?.unwrap().extract()?,
                    indexdef: dict.get_item("indexdef")?.unwrap().extract()?,
                    tablespace: match dict.get_item("tablespace")? {
                        Some(v) if !v.is_none() => Some(v.extract()?),
                        _ => None,
                    },
                    indreloptions: match dict.get_item("indreloptions")? {
                        Some(v) if !v.is_none() => Some(v.extract()?),
                        _ => None,
                    },
                    indisclustered: dict.get_item("indisclustered")?.unwrap().extract()?,
                    indisreplident: dict.get_item("indisreplident")?.unwrap().extract()?,
                    indnullsnotdistinct: dict.get_item("indnullsnotdistinct")?.unwrap().extract()?,
                    parentidx: {
                        let v = dict.get_item("parentidx")?.unwrap();
                        if let Ok(n) = v.extract::<u32>() {
                            n
                        } else {
                            v.extract::<String>()?.parse::<u32>().unwrap_or(0)
                        }
                    },
                    indnkeyattrs: {
                        let v = dict.get_item("indnkeyattrs")?.unwrap();
                        if let Ok(n) = v.extract::<u32>() {
                            n
                        } else {
                            v.extract::<String>()?.parse::<u32>().unwrap_or(0)
                        }
                    },
                    indexconstraint: match dict.get_item("indexconstraint")? {
                        Some(v) if !v.is_none() => {
                            if let Ok(n) = v.extract::<u32>() {
                                Some(n)
                            } else {
                                Some(v.extract::<String>()?.parse::<u32>().unwrap_or(0))
                            }
                        },
                        _ => None,
                    },
                })
            })
            .collect::<PyResult<Vec<_>>>()?
    };
    let comments_val = row.get_item(17)?;
    let comments: Vec<TableComment> = if comments_val.is_none() {
        Vec::new()
    } else {
        let list = comments_val.cast::<PyList>()?;
        list.iter()
            .map(|item| {
                let dict = item.cast::<PyDict>()?;
                let objsubid = match dict.get_item("objsubid")? {
                    Some(v) if !v.is_none() => v.extract::<i32>().unwrap_or(0),
                    _ => 0,
                };
                let description = match dict.get_item("description")? {
                    Some(v) if !v.is_none() => {
                        if let Ok(s) = v.extract::<String>() {
                            s
                        } else {
                            return Ok(TableComment { objsubid, description: String::new() });
                        }
                    },
                    _ => String::new(),
                };
                
                Ok(TableComment { objsubid, description })
            })
            .collect::<PyResult<Vec<_>>>()?
    };
    let distkey_val = row.get_item(18)?;
    let distkey: Option<Vec<String>> = if distkey_val.is_none() {
        None
    } else {
        Some(distkey_val.extract()?)
    };
    let view_definition_val = row.get_item(19)?;
    let view_definition: Option<String> = if view_definition_val.is_none() {
        None
    } else {
        Some(view_definition_val.extract()?)
    };
    let with_data_val = row.get_item(20)?;
    let with_data: bool = if with_data_val.is_none() {
        false
    } else {
        with_data_val.extract()?
    };
    let triggers_val = row.get_item(22)?;
    let triggers: Vec<TriggerInfo> = if triggers_val.is_none() {
        Vec::new()
    } else {
        let list = triggers_val.cast::<PyList>()?;
        list.iter()
            .map(|item| {
                let dict = item.cast::<PyDict>()?;
                Ok(TriggerInfo {
                    tgname: dict.get_item("tgname")?.unwrap().extract()?,
                    tgdef: dict.get_item("tgdef")?.unwrap().extract()?,
                    tgenabled: dict.get_item("tgenabled")?.unwrap().extract()?,
                    tgispartition: dict.get_item("tgispartition")?.unwrap().extract()?,
                })
            })
            .collect::<PyResult<Vec<_>>>()?
    };
    let reloptions_val = row.get_item(5)?;
    let reloptions: Option<Vec<String>> = if reloptions_val.is_none() {
        None
    } else {
        let list = reloptions_val.cast::<PyList>()?;
        let vec: PyResult<Vec<String>> = list.iter().map(|item| item.extract()).collect();
        Some(vec?)
    };
    let access_method_val = row.get_item(23)?;
    let access_method: Option<String> = if access_method_val.is_none() {
        None
    } else {
        Some(access_method_val.extract()?)
    };
    let metadata = TableMetadata {
        oid: row.get_item(0)?.extract()?,
        relname: row.get_item(1)?.extract()?,
        relowner: row.get_item(2)?.extract()?,
        relkind: row.get_item(3)?.extract()?,
        relpersistence: row.get_item(4)?.extract()?,
        reloptions: reloptions,
        reltablespace: row.get_item(6)?.extract::<Option<u32>>()?,
        relacl: row.get_item(7)?.extract::<Option<String>>()?,
        relallfrozen: row.get_item(8)?.extract()?,
        schema_name: row.get_item(9)?.extract()?,
        owner_name: row.get_item(10)?.extract()?,
        partition,
        columns,
        constraints,
        indexes,
        comments,
        distkey,
        view_definition,
        with_data,
        triggers,
        access_method,
    };
    let ddl = generate_table_ddl(&metadata, &opts);
    let metadata_dict = PyDict::new(py);
    metadata_dict.set_item("oid", metadata.oid)?;
    metadata_dict.set_item("relname", metadata.relname)?;
    metadata_dict.set_item("relowner", metadata.relowner)?;
    metadata_dict.set_item("relkind", metadata.relkind)?;
    metadata_dict.set_item("relpersistence", metadata.relpersistence)?;
    metadata_dict.set_item("schema_name", metadata.schema_name)?;
    metadata_dict.set_item("owner_name", metadata.owner_name)?;
    metadata_dict.set_item("columns", struct_to_py_dict(py, &metadata.columns)?)?;
    metadata_dict.set_item("constraints", struct_to_py_dict(py, &metadata.constraints)?)?;
    metadata_dict.set_item("indexes", struct_to_py_dict(py, &metadata.indexes)?)?;
    metadata_dict.set_item("triggers", struct_to_py_dict(py, &metadata.triggers)?)?;
    metadata_dict.set_item("comments", struct_to_py_dict(py, &metadata.comments)?)?;
    metadata_dict.set_item("view_definition", metadata.view_definition)?;
    metadata_dict.set_item("with_data", metadata.with_data)?;
    metadata_dict.set_item("distkey", metadata.distkey)?;
    metadata_dict.set_item("is_partitioned", metadata.partition.is_partitioned)?;
    metadata_dict.set_item("is_partition", metadata.partition.is_partition)?;

    if let Some(ref key) = metadata.partition.partition_key {
        metadata_dict.set_item("partition_key", key)?;
    }

    if let Some(ref bound) = metadata.partition.partition_bound {
        metadata_dict.set_item("partition_bound", bound)?;
    }

    Ok((ddl, metadata_dict.into()))
}
