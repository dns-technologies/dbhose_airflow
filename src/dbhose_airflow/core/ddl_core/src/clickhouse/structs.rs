use serde::{
    Deserialize,
    Serialize,
};
use std::collections::HashMap;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseColumn {
    pub name: String,
    pub data_type: String,
    pub default_expr: Option<String>,
    pub default_kind: Option<String>,
    pub codec: Option<String>,
    pub ttl: Option<String>,
    pub comment: Option<String>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseMetadata {
    pub object_type: String,
    pub database: String,
    pub name: String,
    pub engine: Option<String>,
    pub columns: Vec<ClickHouseColumn>,
    pub partition_by: Option<String>,
    pub order_by: Option<Vec<String>>,
    pub primary_key: Option<Vec<String>>,
    pub sample_by: Option<String>,
    pub ttl: Option<String>,
    pub settings: Option<HashMap<String, String>>,
    pub as_select: Option<String>,
    pub depends_on: Vec<String>,
    pub comment: Option<String>,
}
