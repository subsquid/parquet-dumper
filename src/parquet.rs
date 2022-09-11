use eth_archive_parquet_writer::{IntoRowGroups, BlockNum, Result};
use crate::entities::{Block, Call, Event, Extrinsic};
use arrow2::array::{
    Array, MutableArray, MutableBinaryArray as ArrowMutableBinaryArray,
    Int32Vec, MutableBooleanArray, Int64Vec,
};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::chunk::Chunk as ArrowChunk;
use arrow2::compute::sort::{sort_to_indices, SortOptions};
use arrow2::compute::take::take as arrow_take;

type Chunk = ArrowChunk<Box<dyn Array>>;

type MutableBinaryArray = ArrowMutableBinaryArray<i64>;

fn value_to_string(value: Option<serde_json::Value>) -> Option<String> {
    value.map(|value| serde_json::to_string(&value).unwrap())
}

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

#[derive(Debug, Default)]
pub struct Extrinsics {
    pub id: MutableBinaryArray,
    pub block_id: MutableBinaryArray,
    pub index_in_block: Int32Vec,
    pub version: Int32Vec,
    pub signature: MutableBinaryArray,
    pub call_id: MutableBinaryArray,
    pub fee: Int64Vec,
    pub tip: Int64Vec,
    pub success: MutableBooleanArray,
    pub error: MutableBinaryArray,
    pub hash: MutableBinaryArray,
    pub pos: Int32Vec,
    pub len: usize,
}

impl IntoRowGroups for Extrinsics {
    type Elem = Extrinsic;

    fn schema() -> Schema {
        Schema::from(vec![
            Field::new("id", DataType::Binary, false),
            Field::new("block_id", DataType::Binary, false),
            Field::new("index_in_block", DataType::Int32, false),
            Field::new("version", DataType::Int32, false),
            Field::new("signature", DataType::Binary, true),
            Field::new("call_id", DataType::Binary, false),
            Field::new("fee", DataType::Int64, true),
            Field::new("fee", DataType::Int64, true),
            Field::new("success", DataType::Boolean, false),
            Field::new("error", DataType::Binary, true),
            Field::new("hash", DataType::Binary, false),
            Field::new("pos", DataType::Int32, false),
        ])
    }

    fn into_chunk(mut self) -> Chunk {
        let id = self.id.as_box();

        let indices = sort_to_indices::<i64>(
            id.as_ref(),
            &SortOptions {
                descending: false,
                nulls_first: false,
            },
            None,
        )
        .unwrap();

        Chunk::new(vec![
            arrow_take(id.as_ref(), &indices).unwrap(),
            arrow_take(self.block_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.index_in_block.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.version.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.signature.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.call_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.fee.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.tip.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.success.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.error.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.hash.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.pos.as_box().as_ref(), &indices).unwrap(),
        ])
    }

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.id.push(Some(elem.id));
        self.block_id.push(Some(elem.block_id));
        self.index_in_block.push(Some(elem.index_in_block));
        self.version.push(Some(elem.version));
        self.signature.push(value_to_string(elem.signature));
        self.call_id.push(Some(elem.call_id));
        self.fee.push(elem.fee);
        self.tip.push(elem.tip);
        self.success.push(Some(elem.success));
        self.error.push(value_to_string(elem.error));
        self.hash.push(Some(elem.hash));
        self.pos.push(Some(elem.pos));

        self.len += 1;

        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }
}

#[derive(Debug, Default)]
pub struct Calls {
    pub id: MutableBinaryArray,
    pub parent_id: MutableBinaryArray,
    pub block_id: MutableBinaryArray,
    pub extrinsic_id: MutableBinaryArray,
    pub origin: MutableBinaryArray,
    pub success: MutableBooleanArray,
    pub error: MutableBinaryArray,
    pub name: MutableBinaryArray,
    pub args: MutableBinaryArray,
    pub pos: Int32Vec,
    pub len: usize,
}

impl IntoRowGroups for Calls {
    type Elem = Call;

    fn schema() -> Schema {
        Schema::from(vec![
            Field::new("id", DataType::Binary, false),
            Field::new("parent_id", DataType::Binary, true),
            Field::new("block_id", DataType::Binary, false),
            Field::new("extrinsic_id", DataType::Binary, false),
            Field::new("origin", DataType::Binary, true),
            Field::new("success", DataType::Boolean, false),
            Field::new("error", DataType::Binary, true),
            Field::new("name", DataType::Binary, false),
            Field::new("args", DataType::Binary, true),
            Field::new("pos", DataType::Int32, false),
        ])
    }

    fn into_chunk(mut self) -> Chunk {
        let id = self.id.as_box();

        let indices = sort_to_indices::<i64>(
            id.as_ref(),
            &SortOptions {
                descending: false,
                nulls_first: false,
            },
            None,
        )
        .unwrap();

        Chunk::new(vec![
            arrow_take(id.as_ref(), &indices).unwrap(),
            arrow_take(self.parent_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.block_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.extrinsic_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.origin.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.success.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.error.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.name.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.args.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.pos.as_box().as_ref(), &indices).unwrap(),
        ])
    }

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.id.push(Some(elem.id));
        self.parent_id.push(elem.parent_id);
        self.block_id.push(Some(elem.block_id));
        self.extrinsic_id.push(Some(elem.extrinsic_id));
        self.origin.push(value_to_string(elem.origin));
        self.success.push(Some(elem.success));
        self.error.push(value_to_string(elem.error));
        self.name.push(Some(elem.name));
        self.args.push(value_to_string(elem.args));
        self.pos.push(Some(elem.pos));

        self.len += 1;

        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }
}

#[derive(Debug, Default)]
pub struct Events {
    pub id: MutableBinaryArray,
    pub block_id: MutableBinaryArray,
    pub index_in_block: Int32Vec,
    pub phase: MutableBinaryArray,
    pub extrinsic_id: MutableBinaryArray,
    pub call_id: MutableBinaryArray,
    pub name: MutableBinaryArray,
    pub args: MutableBinaryArray,
    pub pos: Int32Vec,
    pub len: usize,
}

impl IntoRowGroups for Events {
    type Elem = Event;

    fn schema() -> Schema {
        Schema::from(vec![
            Field::new("id", DataType::Binary, false),
            Field::new("block_id", DataType::Binary, false),
            Field::new("index_in_block", DataType::Int32, false),
            Field::new("phase", DataType::Binary, false),
            Field::new("extrinsic_id", DataType::Binary, true),
            Field::new("call_id", DataType::Binary, true),
            Field::new("name", DataType::Binary, false),
            Field::new("args", DataType::Binary, true),
            Field::new("pos", DataType::Int32, false),
        ])
    }

    fn into_chunk(mut self) -> Chunk {
        let id = self.id.as_box();

        let indices = sort_to_indices::<i64>(
            id.as_ref(),
            &SortOptions {
                descending: false,
                nulls_first: false,
            },
            None,
        )
        .unwrap();

        Chunk::new(vec![
            arrow_take(id.as_ref(), &indices).unwrap(),
            arrow_take(self.block_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.index_in_block.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.phase.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.extrinsic_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.call_id.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.name.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.args.as_box().as_ref(), &indices).unwrap(),
            arrow_take(self.pos.as_box().as_ref(), &indices).unwrap(),
        ])
    }

    fn push(&mut self, elem: Self::Elem) -> Result<()> {
        self.id.push(Some(elem.id));
        self.block_id.push(Some(elem.block_id));
        self.index_in_block.push(Some(elem.index_in_block));
        self.phase.push(Some(elem.phase));
        self.extrinsic_id.push(elem.extrinsic_id);
        self.call_id.push(elem.call_id);
        self.name.push(Some(elem.name));
        self.args.push(value_to_string(elem.args));
        self.pos.push(Some(elem.pos));

        self.len += 1;

        Ok(())
    }

    fn len(&self) -> usize {
        self.len
    }
}
