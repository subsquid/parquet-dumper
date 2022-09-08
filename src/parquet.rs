use eth_archive_parquet_writer::{IntoRowGroups, BlockNum, Result};
use crate::entities::{Block, Call, Event, Extrinsic};
use arrow2::array::{
    Array, MutableArray, MutableBinaryArray as ArrowMutableBinaryArray,
    Int32Vec,
};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::chunk::Chunk as ArrowChunk;
use arrow2::compute::sort::{sort_to_indices, SortOptions};
use arrow2::compute::take::take as arrow_take;

type Chunk = ArrowChunk<Box<dyn Array>>;

type MutableBinaryArray = ArrowMutableBinaryArray<i64>;

fn extract_block_num(block_id: &String) -> i32 {
    block_id.split('-').next().unwrap().parse::<i32>().unwrap()
}

impl BlockNum for Block {
    fn block_num(&self) -> i64 {
        self.height.into()
    }
}

impl BlockNum for Call {
    fn block_num(&self) -> i64 {
        extract_block_num(&self.block_id).into()
    }
}

impl BlockNum for Event {
    fn block_num(&self) -> i64 {
        extract_block_num(&self.block_id).into()
    }
}

impl BlockNum for Extrinsic {
    fn block_num(&self) -> i64 {
        extract_block_num(&self.block_id).into()
    }
}

#[derive(Debug, Default)]
pub struct Blocks {
    pub id: MutableBinaryArray,
    pub height: Int32Vec,
    pub hash: MutableBinaryArray,
    pub parent_hash: MutableBinaryArray,
    pub state_root: MutableBinaryArray,
    pub extrinsics_root: MutableBinaryArray,
    pub timestamp: MutableBinaryArray,
    pub spec_id: MutableBinaryArray,
    pub validator: MutableBinaryArray,
    pub len: usize,
}

impl IntoRowGroups for Blocks {
    type Elem = Block;

    fn schema() -> Schema {
        Schema::from(vec![
            Field::new("id", DataType::Binary, false),
            Field::new("height", DataType::Int32, false),
            Field::new("hash", DataType::Binary, false),
            Field::new("parent_hash", DataType::Binary, false),
            Field::new("state_root", DataType::Binary, false),
            Field::new("extrinsics_root", DataType::Binary, false),
            Field::new("timestamp", DataType::Binary, false),
            Field::new("spec_id", DataType::Binary, false),
            Field::new("validator", DataType::Binary, true),
        ])
    }

    fn into_chunk(mut self) -> Chunk {
        let height = self.height.as_box();

        let indices = sort_to_indices::<i64>(
            height.as_ref(),
            &SortOptions {
                descending: false,
                nulls_first: false,
            },
            None,
        )
        .unwrap();

        Chunk::new(vec![
            arrow_take(self.id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(height.as_ref(), &indices).unwrap(),
            arrow_take(self.hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.parent_hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.state_root.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.extrinsics_root.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.timestamp.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.spec_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.validator.as_box().as_ref(), &indices).unwrap(),
        ])
    }

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.id.push(Some(elem.id));
        self.height.push(Some(elem.height));
        self.hash.push(Some(elem.hash));
        self.parent_hash.push(Some(elem.parent_hash));
        self.state_root.push(Some(elem.state_root));
        self.extrinsics_root.push(Some(elem.extrinsics_root));
        self.timestamp.push(Some(elem.timestamp));
        self.spec_id.push(Some(elem.spec_id));
        self.validator.push(elem.validator);

        self.len += 1;

        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }
}
