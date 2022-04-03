use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use ::parquet::file::writer::{SerializedFileWriter, FileWriter};
use ::parquet::file::properties::WriterProperties;
use ::parquet::column::writer::ColumnWriter;
use ::parquet::schema::parser::parse_message_type;
use ::parquet::schema::types::Type;
use ::parquet::data_type::ByteArray;
use crate::entities::{Call, Event, Extrinsic, Block};


pub struct ColumnContext<T> {
    data: Vec<T>,
    def_levels: Vec<i16>,
}


impl<T> ColumnContext<T> {
    fn new() -> Self {
        ColumnContext {
            data: Vec::new(),
            def_levels: Vec::new(),
        }
    }
}


pub enum ContextType<'a> {
    ByteArray(&'a ColumnContext<ByteArray>),
    Int32(&'a ColumnContext<i32>),
    Int64(&'a ColumnContext<i64>),
    Bool(&'a ColumnContext<bool>),
}


pub trait Parquet {
    fn context(&self) -> Vec<ContextType>;
    fn schema(&self) -> Type;
}


struct BlockParquetContext {
    id: ColumnContext<ByteArray>,
    height: ColumnContext<i32>,
    hash: ColumnContext<ByteArray>,
    parent_hash: ColumnContext<ByteArray>,
    timestamp: ColumnContext<i64>,
}


impl BlockParquetContext {
    fn new() -> Self {
        BlockParquetContext {
            id: ColumnContext::new(),
            height: ColumnContext::new(),
            hash: ColumnContext::new(),
            parent_hash: ColumnContext::new(),
            timestamp: ColumnContext::new(),
        }
    }
}


struct ExtrinsicParquetContext {
    id: ColumnContext<ByteArray>,
    block_id: ColumnContext<ByteArray>,
    index_in_block: ColumnContext<i32>,
    name: ColumnContext<ByteArray>,
    signature: ColumnContext<ByteArray>,
    success: ColumnContext<bool>,
    hash: ColumnContext<ByteArray>,
    call_id: ColumnContext<ByteArray>,
}


impl ExtrinsicParquetContext {
    fn new() -> Self {
        ExtrinsicParquetContext {
            id: ColumnContext::new(),
            block_id: ColumnContext::new(),
            index_in_block: ColumnContext::new(),
            name: ColumnContext::new(),
            signature: ColumnContext::new(),
            success: ColumnContext::new(),
            hash: ColumnContext::new(),
            call_id: ColumnContext::new(),
        }
    }
}


struct EventParquetContext {
    id: ColumnContext<ByteArray>,
    block_id: ColumnContext<ByteArray>,
    index_in_block: ColumnContext<i32>,
    phase: ColumnContext<ByteArray>,
    extrinsic_id: ColumnContext<ByteArray>,
    call_id: ColumnContext<ByteArray>,
    name: ColumnContext<ByteArray>,
    args: ColumnContext<ByteArray>,
}


impl EventParquetContext {
    fn new() -> Self {
        EventParquetContext {
            id: ColumnContext::new(),
            block_id: ColumnContext::new(),
            index_in_block: ColumnContext::new(),
            phase: ColumnContext::new(),
            extrinsic_id: ColumnContext::new(),
            call_id: ColumnContext::new(),
            name: ColumnContext::new(),
            args: ColumnContext::new(),
        }
    }
}


struct CallParquetContext {
    id: ColumnContext<ByteArray>,
    index: ColumnContext<i32>,
    extrinsic_id: ColumnContext<ByteArray>,
    parent_id: ColumnContext<ByteArray>,
    success: ColumnContext<bool>,
    name: ColumnContext<ByteArray>,
    args: ColumnContext<ByteArray>,
}


impl CallParquetContext {
    fn new() -> Self {
        CallParquetContext {
            id: ColumnContext::new(),
            index: ColumnContext::new(),
            extrinsic_id: ColumnContext::new(),
            parent_id: ColumnContext::new(),
            success: ColumnContext::new(),
            name: ColumnContext::new(),
            args: ColumnContext::new(),
        }
    }
}


pub struct BlockParquet {
    context: BlockParquetContext,
}


pub struct ExtrinsicParquet {
    context: ExtrinsicParquetContext,
}


pub struct EventParquet {
    context: EventParquetContext,
}


pub struct CallParquet {
    context: CallParquetContext,
}


impl Parquet for BlockParquet {
    fn context(&self) -> Vec<ContextType> {
        Vec::from([
            ContextType::ByteArray(&self.context.id),
            ContextType::Int32(&self.context.height),
            ContextType::ByteArray(&self.context.hash),
            ContextType::ByteArray(&self.context.parent_hash),
            ContextType::Int64(&self.context.timestamp),
        ])
    }

    fn schema(&self) -> Type {
        let message_type = "
            message schema {
                REQUIRED BYTE_ARRAY id;
                REQUIRED INT32 height;
                REQUIRED BYTE_ARRAY hash;
                REQUIRED BYTE_ARRAY parent_hash;
                REQUIRED INT64 timestamp (TIMESTAMP(MILLIS, true));
            }
        ";
        parse_message_type(message_type).unwrap()
    }
}


impl Parquet for ExtrinsicParquet {
    fn context(&self) -> Vec<ContextType> {
        Vec::from([
            ContextType::ByteArray(&self.context.id),
            ContextType::ByteArray(&self.context.block_id),
            ContextType::Int32(&self.context.index_in_block),
            ContextType::ByteArray(&self.context.name),
            ContextType::ByteArray(&self.context.signature),
            ContextType::Bool(&self.context.success),
            ContextType::ByteArray(&self.context.hash),
            ContextType::ByteArray(&self.context.call_id),
        ])
    }

    fn schema(&self) -> Type {
        let message_type = "
            message schema {
                REQUIRED BYTE_ARRAY id;
                REQUIRED BYTE_ARRAY block_id;
                REQUIRED INT32 index_in_block;
                REQUIRED BYTE_ARRAY name;
                OPTIONAL BYTE_ARRAY signature (JSON);
                REQUIRED BOOLEAN success;
                REQUIRED BYTE_ARRAY hash;
                REQUIRED BYTE_ARRAY call_id;
            }
        ";
        parse_message_type(message_type).unwrap()
    }
}


impl Parquet for EventParquet {
    fn context(&self) -> Vec<ContextType> {
        Vec::from([
            ContextType::ByteArray(&self.context.id),
            ContextType::ByteArray(&self.context.block_id),
            ContextType::Int32(&self.context.index_in_block),
            ContextType::ByteArray(&self.context.phase),
            ContextType::ByteArray(&self.context.extrinsic_id),
            ContextType::ByteArray(&self.context.call_id),
            ContextType::ByteArray(&self.context.name),
            ContextType::ByteArray(&self.context.args),
        ])
    }

    fn schema(&self) -> Type {
        let message_type = "
            message schema {
                REQUIRED BYTE_ARRAY id;
                REQUIRED BYTE_ARRAY block_id;
                REQUIRED INT32 index_in_block;
                REQUIRED BYTE_ARRAY phase;
                OPTIONAL BYTE_ARRAY extrinsic_id;
                OPTIONAL BYTE_ARRAY call_id;
                REQUIRED BYTE_ARRAY name;
                OPTIONAL BYTE_ARRAY args (JSON);
            }
        ";
        parse_message_type(message_type).unwrap()
    }
}


impl Parquet for CallParquet {
    fn context(&self) -> Vec<ContextType> {
        Vec::from([
            ContextType::ByteArray(&self.context.id),
            ContextType::Int32(&self.context.index),
            ContextType::ByteArray(&self.context.extrinsic_id),
            ContextType::ByteArray(&self.context.parent_id),
            ContextType::Bool(&self.context.success),
            ContextType::ByteArray(&self.context.name),
            ContextType::ByteArray(&self.context.args),
        ])
    }

    fn schema(&self) -> Type {
        let message_type = "
            message schema {
                REQUIRED BYTE_ARRAY id;
                REQUIRED INT32 index;
                REQUIRED BYTE_ARRAY extrinsic_id;
                OPTIONAL BYTE_ARRAY parent_id;
                REQUIRED BOOLEAN success;
                REQUIRED BYTE_ARRAY name;
                OPTIONAL BYTE_ARRAY args (JSON);
            }
        ";
        parse_message_type(message_type).unwrap()
    }
}


impl BlockParquet {
    pub fn new() -> Self {
        BlockParquet {
            context: BlockParquetContext::new(),
        }
    }

    pub fn insert(&mut self, block: Block) {
        self.context.id.data.push(ByteArray::from(block.id.into_bytes()));
        self.context.height.data.push(block.height);
        self.context.hash.data.push(ByteArray::from(block.hash.into_bytes()));
        self.context.parent_hash.data.push(ByteArray::from(block.parent_hash.into_bytes()));
        self.context.timestamp.data.push(block.timestamp.timestamp_millis());
    }
}


impl ExtrinsicParquet {
    pub fn new() -> Self {
        ExtrinsicParquet {
            context: ExtrinsicParquetContext::new(),
        }
    }

    pub fn insert(&mut self, extrinsic: Extrinsic) {
        self.context.id.data.push(ByteArray::from(extrinsic.id.into_bytes()));
        self.context.block_id.data.push(ByteArray::from(extrinsic.block_id.into_bytes()));
        self.context.index_in_block.data.push(extrinsic.index_in_block);
        self.context.name.data.push(ByteArray::from(extrinsic.name.into_bytes()));
        if let Some(signature) = extrinsic.signature {
            self.context.signature.data.push(ByteArray::from(signature.to_string().into_bytes()));
            self.context.signature.def_levels.push(1);
        } else {
            self.context.signature.def_levels.push(0);
        }
        self.context.success.data.push(extrinsic.success);
        self.context.hash.data.push(ByteArray::from(extrinsic.hash.into_bytes()));
        self.context.call_id.data.push(ByteArray::from(extrinsic.call_id.into_bytes()));
    }
}


impl EventParquet {
    pub fn new() -> Self {
        EventParquet {
            context: EventParquetContext::new(),
        }
    }

    pub fn insert(&mut self, event: Event) {
        self.context.id.data.push(ByteArray::from(event.id.into_bytes()));
        self.context.block_id.data.push(ByteArray::from(event.block_id.into_bytes()));
        self.context.index_in_block.data.push(event.index_in_block);
        self.context.phase.data.push(ByteArray::from(event.phase.into_bytes()));
        if let Some(extrinsic_id) = event.extrinsic_id {
            self.context.extrinsic_id.data.push(ByteArray::from(extrinsic_id.into_bytes()));
            self.context.extrinsic_id.def_levels.push(1);
        } else {
            self.context.extrinsic_id.def_levels.push(0);
        }
        if let Some(call_id) = event.call_id {
            self.context.call_id.data.push(ByteArray::from(call_id.into_bytes()));
            self.context.call_id.def_levels.push(1);
        } else {
            self.context.call_id.def_levels.push(0);
        }
        self.context.name.data.push(ByteArray::from(event.name.into_bytes()));
        if let Some(args) = event.args {
            self.context.args.data.push(ByteArray::from(args.to_string().into_bytes()));
            self.context.args.def_levels.push(1);
        } else {
            self.context.args.def_levels.push(0);
        }
    }
}


impl CallParquet {
    pub fn new() -> Self {
        CallParquet {
            context: CallParquetContext::new(),
        }
    }

    pub fn insert(&mut self, call: Call) {
        self.context.id.data.push(ByteArray::from(call.id.into_bytes()));
        self.context.index.data.push(call.index);
        self.context.extrinsic_id.data.push(ByteArray::from(call.extrinsic_id.into_bytes()));
        if let Some(parent_id) = call.parent_id {
            self.context.parent_id.data.push(ByteArray::from(parent_id.into_bytes()));
            self.context.parent_id.def_levels.push(1);
        } else {
            self.context.parent_id.def_levels.push(0);
        }
        self.context.success.data.push(call.success);
        self.context.name.data.push(ByteArray::from(call.name.into_bytes()));
        if let Some(args) = call.args {
            self.context.args.data.push(ByteArray::from(args.to_string().into_bytes()));
            self.context.args.def_levels.push(1);
        } else {
            self.context.args.def_levels.push(0);
        }
    }
}


pub fn save_parquet(parquet: &impl Parquet, path: &Path) {
    let file = File::create(path).unwrap();
    let schema = Arc::new(parquet.schema());
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();

    let mut col_index = 0;
    let parquet_context = parquet.context();
    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        let column_context = &parquet_context[col_index];
        match col_writer {
            ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                match *column_context {
                    ContextType::ByteArray(context) => {
                        typed_writer.write_batch(&context.data, Some(&context.def_levels), None).unwrap();
                    }
                    _ => panic!("Only ContextType::ByteArray is available")
                }
            }
            ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                match *column_context {
                    ContextType::Int32(context) => {
                        typed_writer.write_batch(&context.data, Some(&context.def_levels), None).unwrap();
                    }
                    _ => panic!("Only ContextType::Int32 is available")
                }
            }
            ColumnWriter::Int64ColumnWriter(ref mut typed_writer) => {
                match *column_context {
                    ContextType::Int64(context) => {
                        typed_writer.write_batch(&context.data, Some(&context.def_levels), None).unwrap();
                    }
                    _ => panic!("Only ContextType::Int64 is available")
                }
            }
            ColumnWriter::BoolColumnWriter(ref mut typed_writer) => {
                match *column_context {
                    ContextType::Bool(context) => {
                        typed_writer.write_batch(&context.data, Some(&context.def_levels), None).unwrap();
                    }
                    _ => panic!("Only ContextType::Bool is available")
                }
            }
            _ => {
                unimplemented!();
            }
        }
        col_index += 1;
        row_group_writer.close_column(col_writer).unwrap();
    }
    writer.close_row_group(row_group_writer).unwrap();
    writer.close().unwrap();
}
