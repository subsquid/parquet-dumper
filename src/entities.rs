use serde::Deserialize;
use chrono::{DateTime, Utc};
use serde_json::Value;


#[derive(Deserialize, Debug)]
pub struct Block {
    pub id: String,
    pub height: i32,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: DateTime<Utc>,
    pub spec_id: String,
    pub validator: Option<String>,
}


#[derive(Deserialize, Debug)]
pub struct Extrinsic {
    pub id: String,
    pub block_id: String,
    pub index_in_block: i32,
    pub signature: Option<Value>,
    pub success: bool,
    pub call_id: String,
    pub hash: String,
    pub pos: i32,
}


#[derive(Deserialize, Debug)]
pub struct Event {
    pub id: String,
    pub block_id: String,
    pub index_in_block: i32,
    pub phase: String,
    pub extrinsic_id: Option<String>,
    pub call_id: Option<String>,
    pub name: String,
    pub args: Option<Value>,
    pub pos: i32,
}


#[derive(Deserialize, Debug)]
pub struct Call {
    pub id: String,
    pub parent_id: Option<String>,
    pub block_id: String,
    pub extrinsic_id: String,
    pub success: bool,
    pub name: String,
    pub args: Option<Value>,
    pub pos: i32,
}


#[derive(Deserialize)]
pub struct BlockData {
    pub header: Block,
    pub extrinsics: Vec<Extrinsic>,
    pub events: Vec<Event>,
    pub calls: Vec<Call>,
}
