use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize, Debug)]
pub struct Block {
    pub id: String,
    pub height: i32,
    pub hash: String,
    pub parent_hash: String,
    pub state_root: String,
    pub extrinsics_root: String,
    pub timestamp: String,
    pub spec_id: String,
    pub validator: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Extrinsic {
    pub id: String,
    pub block_id: String,
    pub index_in_block: i32,
    pub version: i32,
    pub signature: Option<Value>,
    pub call_id: String,
    pub fee: Option<i64>,
    pub tip: Option<i64>,
    pub success: bool,
    pub error: Option<Value>,
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
    pub origin: Option<Value>,
    pub success: bool,
    pub error: Option<Value>,
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
