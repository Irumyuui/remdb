use std::sync::Arc;

use crate::format::key::KeySlice;

use super::{Table, block_iter::BlockIter};

pub struct TableIter {
    table: Arc<Table>,
    block_idx: usize,
    block_iter: BlockIter,
}

impl TableIter {
    pub fn new(table: Arc<Table>) -> Self {
        todo!()
    }
}

impl crate::iterator::Iter for TableIter {
    type KeyType<'a> = KeySlice<'a>;

    async fn is_valid(&self) -> bool {
        self.block_idx < self.table.block_offsets.len()
    }

    async fn key(&self) -> Self::KeyType<'_> {
        todo!()
    }

    async fn next(&mut self) -> crate::error::Result<()> {
        todo!()
    }

    async fn value(&self) -> crate::format::value::Value {
        todo!()
    }
}
