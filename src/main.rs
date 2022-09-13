use std::io;
use std::path::PathBuf;
use clap::Parser;
use entities::BlockData;
use eth_archive_parquet_writer::{ParquetWriter, ParquetConfig, BlockRange};
use tokio::sync::mpsc;
use crate::parquet::{Blocks, Extrinsics, Calls, Events};
use crate::sqlite::SQLite;

mod entities;
mod parquet;
mod sqlite;
mod error;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// An output directory for parquet files
    #[clap(short, long)]
    out_dir: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let (tx, _rx) = mpsc::unbounded_channel();
    let block_config = ParquetConfig {
        name: "block".to_string(),
        path: PathBuf::from(&args.out_dir),
        channel_size: 100,
        items_per_file: 4096,
        items_per_row_group: 64,
    };
    let block_writer: ParquetWriter<Blocks> = ParquetWriter::new(block_config, tx.clone());

    let extrinsic_config = ParquetConfig {
        name: "extrinsic".to_string(),
        path: PathBuf::from(&args.out_dir),
        channel_size: 100,
        items_per_file: 4096,
        items_per_row_group: 64,
    };
    let extrinsic_writer: ParquetWriter<Extrinsics> = ParquetWriter::new(extrinsic_config, tx.clone());

    let call_config = ParquetConfig {
        name: "call".to_string(),
        path: PathBuf::from(&args.out_dir),
        channel_size: 100,
        items_per_file: 4096,
        items_per_row_group: 64,
    };
    let call_writer: ParquetWriter<Calls> = ParquetWriter::new(call_config, tx.clone());

    let event_config = ParquetConfig {
        name: "event".to_string(),
        path: PathBuf::from(&args.out_dir),
        channel_size: 100,
        items_per_file: 4096,
        items_per_row_group: 64,
    };
    let event_writer: ParquetWriter<Events> = ParquetWriter::new(event_config, tx);

    let sqlite_path = std::path::Path::new(&args.out_dir).join("metadata.sqlite");
    let sqlite = SQLite::new(&sqlite_path).unwrap();
    sqlite.init_schema().unwrap();

    loop {
        let mut line = String::new();
        io::stdin().read_line(&mut line).unwrap();
        let block_data: BlockData = serde_json::from_str(&line).unwrap();

        let block_range = BlockRange {
            from: usize::try_from(block_data.header.height).unwrap(),
            to: usize::try_from(block_data.header.height).unwrap(),
        };
        block_writer.send((block_range, vec![block_data.header])).await;
        extrinsic_writer.send((block_range, block_data.extrinsics)).await;
        call_writer.send((block_range, block_data.calls)).await;
        event_writer.send((block_range, block_data.events)).await;
        if let Some(metadata) = &block_data.metadata {
            sqlite.insert_metadata(metadata).unwrap();
        }
    }
}
