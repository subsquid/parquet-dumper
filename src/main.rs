use std::{fs, io};
use std::path::PathBuf;
use clap::Parser;
use entities::BlockData;
use crate::parquet::{BlockParquet, CallParquet, EventParquet, ExtrinsicParquet, save_parquet};

mod entities;
mod parquet;


const ZERO_BLOCK: i32 = 0;


#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// An output directory for parquet files
    #[clap(short, long)]
    out_dir: String,
    /// Count of blocks per file
    #[clap(short, long)]
    capacity: i32
}


fn main() -> Result<(), io::Error> {
    let args = Args::parse();
    let out_dir = PathBuf::from(&args.out_dir);
    if !out_dir.exists() {
        fs::create_dir(&args.out_dir).unwrap();
    }

    let block_out_dir = out_dir.join("block");
    if !block_out_dir.exists() {
        fs::create_dir(&block_out_dir).unwrap();
    }

    let extrinsic_out_dir = out_dir.join("extrinsic");
    if !extrinsic_out_dir.exists() {
        fs::create_dir(&extrinsic_out_dir).unwrap();
    }

    let event_out_dir = out_dir.join("event");
    if !event_out_dir.exists() {
        fs::create_dir(&event_out_dir).unwrap();
    }

    let call_out_dir = out_dir.join("call");
    if !call_out_dir.exists() {
        fs::create_dir(&call_out_dir).unwrap();
    }

    let mut block_parquet = BlockParquet::new();
    let mut extrinsic_parquet = ExtrinsicParquet::new();
    let mut event_parquet = EventParquet::new();
    let mut call_parquet = CallParquet::new();
    loop {
        let mut line = String::new();
        io::stdin().read_line(&mut line).unwrap();
        let block_data: BlockData = serde_json::from_str(&line).unwrap();
        let block_height = block_data.header.height;
        block_parquet.insert(block_data.header);
        for extrinsic in block_data.extrinsics {
            extrinsic_parquet.insert(extrinsic);
        }
        for event in block_data.events {
            event_parquet.insert(event);
        }
        for call in block_data.calls {
            call_parquet.insert(call);
        }
        if block_height % args.capacity == 0 && block_height != ZERO_BLOCK {
            let block_path = block_out_dir.join(format!("{}.parquet", block_height));
            let extrinsic_path = extrinsic_out_dir.join(format!("{}.parquet", block_height));
            let event_path = event_out_dir.join(format!("{}.parquet", block_height));
            let call_path = call_out_dir.join(format!("{}.parquet", block_height));
            save_parquet(&block_parquet, &block_path)?;
            save_parquet(&extrinsic_parquet, &extrinsic_path)?;
            save_parquet(&event_parquet, &event_path)?;
            save_parquet(&call_parquet, &call_path)?;
            block_parquet = BlockParquet::new();
            extrinsic_parquet = ExtrinsicParquet::new();
            event_parquet = EventParquet::new();
            call_parquet = CallParquet::new();
        }
    }
}
