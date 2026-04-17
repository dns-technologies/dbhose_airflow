use serde::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclItem {
    pub grantee: String,
    pub grantor: String,
    pub privileges: Vec<String>,
    pub with_grant_option: bool,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub attnum: i32,
    pub attname: String,
    pub atttypid: u32,
    pub attnotnull: bool,
    pub atthasdef: bool,
    pub attoptions: Option<String>,
    pub attfdwoptions: Option<String>,
    pub typname: String,
    pub typnamespace: u32,
    pub defaultval: Option<String>,
    pub attidentity: String,
    pub attgenerated: String,
    pub attcompression: String,
    pub attmissingval: Option<String>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintInfo {
    pub conname: String,
    pub contype: String,
    pub condef: String,
    pub conperiod: bool,
}


#[derive(Debug, Clone)]
pub struct DdlOptions {
    pub include_indexes: bool,
    pub include_constraints_fk: bool,
    pub include_constraints_check: bool,
    pub include_owner: bool,
    pub include_comments: bool,
    pub include_acl: bool,
    pub include_distributed_by: bool,
    pub include_partitions: bool,
    pub include_triggers: bool,
}


impl Default for DdlOptions {
    fn default() -> Self {
        Self {
            include_indexes: true,
            include_constraints_fk: true,
            include_constraints_check: true,
            include_owner: true,
            include_comments: true,
            include_acl: true,
            include_distributed_by: true,
            include_partitions: true,
            include_triggers: true,
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub indexname: String,
    pub indexdef: String,
    pub tablespace: Option<String>,
    pub indreloptions: Option<String>,
    pub indisclustered: bool,
    pub indisreplident: bool,
    pub indnullsnotdistinct: bool,
    pub parentidx: u32,
    pub indnkeyattrs: u32,
    pub indexconstraint: Option<u32>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentTable {
    pub parent_schema: String,
    pub parent_table: String,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub is_partitioned: bool,
    pub is_partition: bool,
    pub partition_bound: Option<String>,
    pub partition_key: Option<String>,
    pub parents: Vec<ParentTable>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceMetadata {
    pub seqname: String,
    pub seqschema: String,
    pub seqdef: String,
    pub owner: String,
    pub owned_by: Option<String>,
    pub relacl: Option<String>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableComment {
    pub objsubid: i32,
    pub description: String,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub oid: u32,
    pub relname: String,
    pub relowner: u32,
    pub relkind: String,
    pub relpersistence: String,
    pub reloptions: Option<Vec<String>>,
    pub reltablespace: Option<u32>,
    pub relacl: Option<String>,
    pub relallfrozen: u32,
    pub schema_name: String,
    pub owner_name: String,
    pub columns: Vec<ColumnInfo>,
    pub constraints: Vec<ConstraintInfo>,
    pub indexes: Vec<IndexInfo>,
    pub partition: PartitionInfo,
    pub comments: Vec<TableComment>,
    pub distkey: Option<Vec<String>>,
    pub view_definition: Option<String>,
    pub with_data: bool,
    pub triggers: Vec<TriggerInfo>,
    pub access_method: Option<String>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerInfo {
    pub tgname: String,
    pub tgdef: String,
    pub tgenabled: String,
    pub tgispartition: bool,
}
