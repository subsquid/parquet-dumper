use std::fs::{File, create_dir};
use std::path::PathBuf;
use std::io::{BufReader, BufRead};
use std::sync::Arc;
use clap::Parser;
use serde::Deserialize;
use parquet::file::writer::{SerializedFileWriter, FileWriter};
use parquet::file::properties::WriterProperties;
use parquet::column::writer::ColumnWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type;
use parquet::data_type::ByteArray;


#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// A file name which are filling by substrate-archive
    #[clap(short, long)]
    file: String,
    /// An output directory for parquet files
    #[clap(short, long)]
    out_dir: String,
    /// Count of blocks per file
    #[clap(short, long)]
    capacity: u32
}


#[derive(Deserialize)]
struct Block {
    id: String,
    height: i32,
    hash: String,
    parent_hash: String,
    timestamp: String,
}


#[derive(Deserialize)]
struct BlockData {
    header: Block,
}


enum ColumnData<'a> {
    ByteArray(&'a Vec<parquet::data_type::ByteArray>),
    Int32(&'a Vec<i32>),
}


trait Parquet {
    fn data(&self) -> Vec<ColumnData>;
    fn schema(&self) -> Type;
}


struct BlockParquetData {
    id: Vec<ByteArray>,
    height: Vec<i32>,
    hash: Vec<ByteArray>,
    parent_hash: Vec<ByteArray>,
    timestamp: Vec<ByteArray>,
}


impl BlockParquetData {
    fn new() -> Self {
        BlockParquetData {
            id: Vec::new(),
            height: Vec::new(),
            hash: Vec::new(),
            parent_hash: Vec::new(),
            timestamp: Vec::new(),
        }
    }
}


struct BlockParquet {
    data: BlockParquetData,
}


impl Parquet for BlockParquet {
    fn data(&self) -> Vec<ColumnData> {
        Vec::from([
            ColumnData::ByteArray(&self.data.id),
            ColumnData::Int32(&self.data.height),
            ColumnData::ByteArray(&self.data.hash),
            ColumnData::ByteArray(&self.data.parent_hash),
            ColumnData::ByteArray(&self.data.timestamp),
        ])
    }

    fn schema(&self) -> Type {
        // TODO: fix timestamp
        let message_type = "
            message schema {
                REQUIRED BYTE_ARRAY id;
                REQUIRED INT32 height;
                REQUIRED BYTE_ARRAY hash;
                REQUIRED BYTE_ARRAY parent_hash;
                REQUIRED BYTE_ARRAY timestamp;
            }
        ";
        parse_message_type(message_type).unwrap()
    }
}


impl BlockParquet {
    fn new() -> Self {
        BlockParquet {
            data: BlockParquetData::new(),
        }
    }

    fn insert(&mut self, block: Block) {
        self.data.id.push(ByteArray::from(block.id.into_bytes()));
        self.data.height.push(block.height);
        self.data.hash.push(ByteArray::from(block.hash.into_bytes()));
        self.data.parent_hash.push(ByteArray::from(block.parent_hash.into_bytes()));
        self.data.timestamp.push(ByteArray::from(block.timestamp.into_bytes()));
    }
}


fn save_parquet(parquet: &impl Parquet, path: &PathBuf) {
    let file = File::create(path).unwrap();
    let schema = Arc::new(parquet.schema());
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut row_group_writer = writer.next_row_group().unwrap();

    let mut col_index = 0;
    let parquet_data = parquet.data();
    while let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        let column_data = &parquet_data[col_index];
        match col_writer {
            ColumnWriter::ByteArrayColumnWriter(ref mut typed_writer) => {
                match *column_data {
                    ColumnData::ByteArray(data) => {
                        typed_writer.write_batch(data, None, None).unwrap();
                    }
                    _ => panic!("Only ColumnData::ByteArray is available")
                }
            }
            ColumnWriter::Int32ColumnWriter(ref mut typed_writer) => {
                match *column_data {
                    ColumnData::Int32(data) => {
                        typed_writer.write_batch(data, None, None).unwrap();
                    }
                    _ => panic!("Only ColumnData::Int32 is available")
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


fn main() {
    let args = Args::parse();
    let out_dir = PathBuf::from(&args.out_dir);
    if !out_dir.exists() {
        create_dir(&args.out_dir).unwrap();
    }

    let block_out_dir = out_dir.join("block");
    if !block_out_dir.exists() {
        create_dir(&block_out_dir).unwrap();
    }

    let file = File::open(args.file).unwrap();
    let mut reader = BufReader::new(file);
    let mut counter = 0;

    let mut block_parquet = BlockParquet::new();

    loop {
        counter += 1;
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let block_data: BlockData = serde_json::from_str(&line).unwrap();

        block_parquet.insert(block_data.header);

        if counter % args.capacity == 0 {
            let block_path = block_out_dir.join(format!("{}.parquet", counter));
            println!("{:?}", &block_path);
            save_parquet(&block_parquet, &block_path);

            block_parquet = BlockParquet::new();
        }
    }

}
