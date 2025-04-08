use std::ops::Bound;

use bytes::Bytes;

use crate::{
    error::KvResult,
    format::key::Seq,
    kv_iter::KvIter,
    memtable::MemTableIter,
    table::table_iter::{TableConcatIter, TableIter},
};

use super::{KvItem, Peekable, prelude::*};

pub type DbMergeIterInner = TwoMergeIter<
    TwoMergeIter<MergeIter<MemTableIter>, MergeIter<TableIter>>,
    MergeIter<TableConcatIter>,
>;

pub struct DbMergeIter {
    inner: DbMergeIterInner,
    upper: Bound<Bytes>,
    read_ts: Seq,
    out_of_bound: bool,
    prev_key: Bytes,
}

impl DbMergeIter {
    pub(crate) async fn new(
        iter: DbMergeIterInner,
        upper: Bound<Bytes>,
        read_ts: Seq,
    ) -> KvResult<Self> {
        let mut this = Self {
            inner: iter,
            upper,
            read_ts,
            out_of_bound: false,
            prev_key: Bytes::default(),
        };
        this.move_to_below_read_ts_item().await?;
        Ok(this)
    }

    fn is_valid(&self) -> bool {
        !self.out_of_bound && self.inner.peek().is_some()
    }

    async fn advance(&mut self) -> KvResult<Option<KvItem>> {
        let current = self.inner.next().await?;

        if let Some(item) = self.inner.peek() {
            self.out_of_bound = match &self.upper {
                Bound::Included(key) => item.key.key() <= key.as_ref(),
                Bound::Excluded(key) => item.key.key() < key.as_ref(),
                Bound::Unbounded => false,
            }
        } else {
            self.out_of_bound = true;
        }
        Ok(current)
    }

    // 移动到read_ts下的第一个元素，移动的是 peek
    async fn move_to_below_read_ts_item(&mut self) -> KvResult<()> {
        'main: loop {
            while let Some(peek_item) = self.peek()
                && self.prev_key == peek_item.key.key()
            {
                self.advance().await?;
            }

            if self.peek().is_none() {
                break 'main;
            }

            self.prev_key = self.peek().unwrap().key.real_key.clone();
            while let Some(peek_item) = self.inner.peek()
                && peek_item.key.key() == self.prev_key
                && peek_item.key.seq() > self.read_ts
            {
                self.advance().await?;
            }
            match self.peek() {
                Some(item) => {
                    if item.key.key() != self.prev_key {
                        continue 'main;
                    } else if !item.value.is_empty() {
                        break 'main;
                    }
                }
                None => break 'main,
            }
        }

        Ok(())
    }
}

impl KvIter for DbMergeIter {
    async fn next(&mut self) -> KvResult<Option<KvItem>> {
        let current = self.advance().await?;
        self.move_to_below_read_ts_item().await?;
        Ok(current)
    }
}

impl Peekable for DbMergeIter {
    fn peek(&self) -> Option<&KvItem> {
        self.inner.peek()
    }
}
