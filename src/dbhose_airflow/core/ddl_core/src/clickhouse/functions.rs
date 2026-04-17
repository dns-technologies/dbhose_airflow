use pyo3::prelude::*;
use pyo3::types::{PyDict, PyAny};
use regex::Regex;
use std::collections::HashMap;

use crate::common::struct_to_py_dict;
use super::structs::{
    ClickHouseColumn,
    ClickHouseMetadata,
};


fn extract_columns_section(ddl: &str) -> Option<String> {
    let start = ddl.find('(')?;
    let mut depth = 1;
    let mut end = start;

    for (i, ch) in ddl[start + 1..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = start + 1 + i;
                    break;
                }
            }
            _ => {}
        }
    }

    if depth == 0 {
        let columns_part = ddl[start + 1..end].trim().to_string();
        let re_index = Regex::new(r"(?i)\s*INDEX\s+\w+\s+").unwrap();
        if let Some(idx_mat) = re_index.find(&columns_part) {
            return Some(columns_part[..idx_mat.start()].trim().to_string());
        }

        Some(columns_part)
    } else {
        None
    }
}


fn parse_column_definition(col_str: &str) -> PyResult<ClickHouseColumn> {
    let mut col = ClickHouseColumn {
        name: String::new(),
        data_type: String::new(),
        default_expr: None,
        default_kind: None,
        codec: None,
        ttl: None,
        comment: None,
    };
    let trimmed = col_str.trim();
    let mut pos = 0;
    let bytes = trimmed.as_bytes();
    let re_name = Regex::new(
        r#"^("(?:\\.|[^"\\])*"|`(?:\\.|[^`\\])*`|\w+)"#,
    ).unwrap();

    if let Some(caps) = re_name.captures(&trimmed[pos..]) {
        let name_match = &caps[1];
        col.name = name_match.trim_matches('"').trim_matches('`').to_string();
        pos += caps[0].len();
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
    }

    let rest_after_name = &trimmed[pos..];
    let keywords = Regex::new(r"(?i)\s+(COMMENT|CODEC|TTL|DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS)(\s+|$)").unwrap();
    let type_end = if let Some(m) = keywords.find(rest_after_name) {
        m.start()
    } else {
        rest_after_name.len()
    };
    col.data_type = rest_after_name[..type_end].trim().to_string();
    pos += type_end;

    while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }

    let re_default = Regex::new(
        r"(?i)^(DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS)\s+",
    ).unwrap();

    if let Some(caps) = re_default.captures(&trimmed[pos..]) {
        col.default_kind = Some(caps[1].to_uppercase());
        pos += caps[0].len();
        let rest = &trimmed[pos..];
        let re_expr_end = Regex::new(
            r"(?i)\s+(CODEC|TTL|COMMENT)(\s+|$)",
        ).unwrap();

        if let Some(end_caps) = re_expr_end.find(rest) {
            let expr_end = end_caps.start();
            col.default_expr = Some(rest[..expr_end].trim().to_string());
            pos += expr_end;
        } else {
            col.default_expr = Some(rest.trim().to_string());
            pos = trimmed.len();
        }
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
    }

    let re_codec_start = Regex::new(r"(?i)^CODEC\s*\(").unwrap();

    if re_codec_start.is_match(&trimmed[pos..]) {
        pos += re_codec_start.find(&trimmed[pos..]).unwrap().end();
        let mut depth = 1;
        let mut end = pos;

        for (i, ch) in trimmed[pos..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = pos + i;
                        break;
                    }
                }
                _ => {}
            }
        }

        col.codec = Some(trimmed[pos..end].trim().to_string());
        pos = end + 1;
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
    }

    let re_ttl = Regex::new(r"(?i)^TTL\s+").unwrap();

    if re_ttl.is_match(&trimmed[pos..]) {
        pos += re_ttl.find(&trimmed[pos..]).unwrap().end();
        let rest = &trimmed[pos..];
        let re_comment = Regex::new(r"(?i)\s+COMMENT\s+").unwrap();

        if let Some(comment_match) = re_comment.find(rest) {
            col.ttl = Some(rest[..comment_match.start()].trim().to_string());
            pos += comment_match.start();
        } else {
            col.ttl = Some(rest.trim().to_string());
            pos = trimmed.len();
        }
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
    }

    if pos < trimmed.len() {
        let rest = &trimmed[pos..];
        let re_comment = Regex::new(r#"(?i)^COMMENT\s+['"]([^'"]*)['"]"#).unwrap();
        if let Some(caps) = re_comment.captures(rest) {
            col.comment = Some(caps[1].to_string());
            pos += caps[0].len();
            while pos < bytes.len() && bytes[pos].is_ascii_whitespace() { pos += 1; }
        }
    }

    if pos < trimmed.len() {
        let re_codec_start2 = Regex::new(r"(?i)^CODEC\s*\(").unwrap();

        if re_codec_start2.is_match(&trimmed[pos..]) {
            pos += re_codec_start2.find(&trimmed[pos..]).unwrap().end();
            let mut depth = 1;
            let mut end = pos;
            for (i, ch) in trimmed[pos..].char_indices() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            end = pos + i;
                            break;
                        }
                    }
                    _ => {}
                }
            }

            col.codec = Some(trimmed[pos..end].trim().to_string());
        }
    }

    Ok(col)
}


fn parse_columns(columns_str: &str) -> PyResult<Vec<ClickHouseColumn>> {
    let mut columns = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    let mut in_quotes = false;
    let mut quote_char = ' ';

    for ch in columns_str.chars() {
        match ch {
            '"' | '\'' | '`' if !in_quotes => {
                in_quotes = true;
                quote_char = ch;
                current.push(ch);
            }
            c if in_quotes && c == quote_char => {
                in_quotes = false;
                current.push(ch);
            }
            '(' if !in_quotes => {
                depth += 1;
                current.push(ch);
            }
            ')' if !in_quotes => {
                depth -= 1;
                current.push(ch);
            }
            ',' if !in_quotes && depth == 0 => {
                if !current.trim().is_empty() {
                    columns.push(parse_column_definition(&current)?);
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        columns.push(parse_column_definition(&current)?);
    }

    Ok(columns)
}


fn extract_engine(ddl: &str) -> Option<String> {
    let re = Regex::new(r"(?i)ENGINE\s*=\s*(\w+)").unwrap();
    re.captures(ddl).map(|caps| caps[1].to_string())
}


fn extract_clause(ddl: &str, clause: &str) -> Option<String> {
    let pattern = format!(r"(?i){}\s+", clause);
    let re_start = Regex::new(&pattern).unwrap();
    
    if let Some(mat) = re_start.find(ddl) {
        let start = mat.end();
        let rest = &ddl[start..];
        let keywords = [
            "ORDER BY",
            "PRIMARY KEY",
            "SAMPLE BY",
            "TTL",
            "SETTINGS",
            "COMMENT",
            "PARTITION BY",
        ];
        let mut end = rest.len();

        for kw in keywords.iter() {
            let kw_pattern = format!(r"(?i)\s+{}\s+", kw);
            let re_kw = Regex::new(&kw_pattern).unwrap();
            if let Some(kw_mat) = re_kw.find(rest) {
                if kw_mat.start() < end {
                    end = kw_mat.start();
                }
            }
        }

        Some(rest[..end].trim().to_string())
    } else {
        None
    }
}


fn extract_order_by(ddl: &str) -> Option<Vec<String>> {
    let clause = extract_clause(ddl, "ORDER BY")?;
    let inner = clause.trim_matches(|c| c == '(' || c == ')');
    Some(inner.split(',').map(|s| s.trim().to_string()).collect())
}


fn extract_primary_key(ddl: &str) -> Option<Vec<String>> {
    let clause = extract_clause(ddl, "PRIMARY KEY")?;
    let inner = clause.trim_matches(|c| c == '(' || c == ')');
    Some(inner.split(',').map(|s| s.trim().to_string()).collect())
}


fn extract_settings(ddl: &str) -> Option<HashMap<String, String>> {
    let re = Regex::new(r"(?i)SETTINGS\s+").unwrap();

    if let Some(mat) = re.find(ddl) {
        let start = mat.end();
        let rest = &ddl[start..];
        let re_end = Regex::new(r"(?i);|COMMENT\s+").unwrap();
        let end = re_end.find(rest).map(
            |m| m.start()
        ).unwrap_or(rest.len());
        let settings_str = &rest[..end];
        let mut settings = HashMap::new();

        for part in settings_str.split(',') {
            let part = part.trim();
            if part.is_empty() { continue; }
            let kv: Vec<&str> = part.splitn(2, '=').collect();
            if kv.len() == 2 {
                settings.insert(
                    kv[0].trim().to_string(),
                    kv[1].trim().to_string(),
                );
            }
        }
        Some(settings)
    } else {
        None
    }
}


fn extract_as_select(ddl: &str) -> Option<String> {
    let re = Regex::new(r"(?i)AS\s+SELECT\s+").unwrap();

    if let Some(mat) = re.find(ddl) {
        let start = mat.end();
        let select_part = &ddl[start..];

        if let Some(end) = select_part.find(';') {
            Some(format!("SELECT {}", select_part[..end].trim()))
        } else {
            Some(format!("SELECT {}", select_part.trim()))
        }
    } else {
        None
    }
}


fn parse_clickhouse_ddl(ddl: &str) -> PyResult<ClickHouseMetadata> {
    let mut meta = ClickHouseMetadata {
        object_type: String::new(),
        database: String::new(),
        name: String::new(),
        engine: None,
        columns: Vec::new(),
        partition_by: None,
        order_by: None,
        primary_key: None,
        sample_by: None,
        ttl: None,
        settings: None,
        as_select: None,
        depends_on: Vec::new(),
        comment: None,
    };

    let re_create = Regex::new(concat!(
        r"(?i)^CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY\s+)?",
        r"(MATERIALIZED\s+VIEW|VIEW|TABLE|DICTIONARY|PROJECTION)",
        r"\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)",
    )).unwrap();

    if let Some(caps) = re_create.captures(ddl) {
        meta.object_type = caps[1].to_string();
        
        let full_name = &caps[2];
        if let Some((db, table)) = full_name.split_once('.') {
            meta.database = db.to_string();
            meta.name = table.to_string();
        } else {
            meta.name = full_name.to_string();
        }
    }

    if let Some(columns_str) = extract_columns_section(ddl) {
        meta.columns = parse_columns(&columns_str)?;
    }

    meta.engine = extract_engine(ddl);
    meta.partition_by = extract_clause(ddl, "PARTITION BY");
    meta.order_by = extract_order_by(ddl);
    meta.primary_key = extract_primary_key(ddl);
    meta.sample_by = extract_clause(ddl, "SAMPLE BY");
    meta.ttl = extract_clause(ddl, "TTL");
    meta.settings = extract_settings(ddl);
    meta.as_select = extract_as_select(ddl);

    if let Some(to_table) = extract_clause(ddl, "TO") {
        meta.depends_on.push(to_table);
    }

    let re_comment = Regex::new(r#"(?i)COMMENT\s+['"]([^'"]*)['"]"#).unwrap();

    if let Some(caps) = re_comment.captures(ddl) {
        meta.comment = Some(caps[1].to_string());
    }

    let re_table_comment = Regex::new(r#"(?i)COMMENT\s+['"]([^'"]*)['"]\s*;?\s*$"#).unwrap();

    if let Some(caps) = re_table_comment.captures(ddl) {
        meta.comment = Some(caps[1].to_string());
    }

    Ok(meta)
}


#[pyfunction]
pub fn clickhouse_ddl(
    py: Python<'_>,
    cursor: Py<PyAny>,
    object_name: String,
) -> PyResult<(String, Py<PyDict>)> {
    let cursor_bound = cursor.bind(py);
    let is_readonly: bool = cursor_bound.getattr("is_readonly")?.extract()?;
    let object_type: String;

    if !is_readonly {
        let query = format!(
            r#"
            SELECT multiIf(
                engine IN ('View', 'MaterializedView'), 'View',
                engine = 'Dictionary', 'Dictionary',
                'Table'
            ) AS object_type
            FROM system.tables
            WHERE concat(database, '.', name) = '{}'
            "#,
            object_name.replace("'", "''")
        );
        let reader = cursor_bound.call_method1("get_stream", (query.as_str(),))?;
        let rows = reader.call_method0("to_rows")?;
        let row = match rows.call_method0("__next__") {
            Ok(r) => r,
            Err(_) => {
                reader.call_method0("close")?;
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    format!("Object '{}' not found", object_name)
                ));
            }
        };
        object_type = row.get_item(0)?.extract::<String>()?;
        reader.call_method0("close")?;
    } else {
        let mut found = false;
        let mut obj_type_str = String::new();

        for obj_type in ["Table", "View", "Dictionary"] {
            let query = format!("EXISTS {} {}", obj_type, object_name);
            let reader = cursor_bound.call_method1("get_stream", (query.as_str(),))?;
            let rows = reader.call_method0("to_rows")?;

            if let Ok(row) = rows.call_method0("__next__") {
                let exists: u8 = row.get_item(0)?.extract()?;
                reader.call_method0("close")?;

                if exists == 1 {
                    obj_type_str = obj_type.to_string();
                    found = true;
                    break;
                }
            } else {
                reader.call_method0("close")?;
            }
        }

        if !found {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Object '{}' not found", object_name)
            ));
        }

        object_type = obj_type_str;
    }

    let query = format!("SHOW CREATE {} {}", object_type, object_name);
    let reader = cursor_bound.call_method1("get_stream", (query.as_str(),))?;
    let rows = reader.call_method0("to_rows")?;
    let row = match rows.call_method0("__next__") {
        Ok(r) => r,
        Err(_) => {
            reader.call_method0("close")?;
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Failed to get DDL for '{}'", object_name)
            ));
        }
    };
    let ddl: String = row.get_item(0)?.extract()?;
    reader.call_method0("close")?;
    let metadata = parse_clickhouse_ddl(&ddl)?;
    let metadata_dict = PyDict::new(py);
    metadata_dict.set_item("object_type", &metadata.object_type)?;
    metadata_dict.set_item("database", &metadata.database)?;
    metadata_dict.set_item("name", &metadata.name)?;
    metadata_dict.set_item("engine", &metadata.engine)?;
    metadata_dict.set_item("columns", struct_to_py_dict(py, &metadata.columns)?)?;
    metadata_dict.set_item("partition_by", &metadata.partition_by)?;
    metadata_dict.set_item("order_by", &metadata.order_by)?;
    metadata_dict.set_item("primary_key", &metadata.primary_key)?;
    metadata_dict.set_item("sample_by", &metadata.sample_by)?;
    metadata_dict.set_item("ttl", &metadata.ttl)?;
    metadata_dict.set_item("settings", struct_to_py_dict(py, &metadata.settings)?)?;
    metadata_dict.set_item("as_select", &metadata.as_select)?;
    metadata_dict.set_item("depends_on", &metadata.depends_on)?;
    metadata_dict.set_item("comment", &metadata.comment)?;
    Ok((ddl, metadata_dict.into()))
}
