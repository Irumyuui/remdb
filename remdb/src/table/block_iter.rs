use crate::format::{
    key::{KeyBuf, KeyBytes, KeySlice},
    value::Value,
};

use super::block::{Block, Entry};

pub struct BlockIter {
    block: Block,
    entry_idx: usize,

    current_key: KeyBuf,
    current_entry: Option<Entry>,
}

impl BlockIter {
    /// create a new iter, but not valid
    pub fn new(block: Block) -> Self {
        Self {
            block,
            entry_idx: 0,
            current_key: Default::default(),
            current_entry: None,
        }
    }

    pub fn seek_to_first(&mut self) {
        self.seek_to_index(0);
    }

    pub fn seek_to_last(&mut self) {
        self.seek_to_index(self.block.entry_count() - 1);
    }

    fn seek_to_index(&mut self, index: usize) {
        tracing::debug!("seek to index: {}", index);

        self.entry_idx = index;
        if index < self.block.entry_count() {
            self.current_entry.replace(self.block.get_entry(index));
            self.init_current_key();
        }
    }

    pub fn is_valid(&self) -> bool {
        self.current_entry.is_some() && self.entry_idx < self.block.entry_count()
    }

    fn is_first_key(&self) -> bool {
        self.entry_idx == 0
    }

    fn init_current_key(&mut self) {
        assert!(self.is_valid());

        let entry = self.current_entry.as_ref().unwrap();
        if self.is_first_key() {
            self.current_key.clear();
            self.current_key.append(&entry.diff_key);
            self.current_key.set_seq(entry.header.seq);
        } else {
            let base_key = self.block.base_key();
            let overlap = entry.header.overlap as usize;
            let diff = entry.header.diff_len as usize;

            let target_key_len = overlap + diff;
            self.current_key.clear();
            self.current_key.append(&base_key[..overlap]);
            self.current_key.append(&entry.diff_key);
            self.current_key.set_seq(entry.header.seq);
        }

        tracing::debug!("init current key: {:?}", self.current_key);
    }

    pub fn key(&self) -> KeySlice {
        assert!(self.is_valid());
        self.current_key.as_key_slice()
    }

    pub fn value(&self) -> Value {
        assert!(self.is_valid());
        self.current_entry.as_ref().unwrap().value()
    }

    pub fn next(&mut self) {
        let next = self.block.entry_count().min(self.entry_idx + 1);
        self.seek_to_index(next);
    }

    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut l = 0;
        let mut r = self.block.entry_count();

        while l < r {
            let mid = l + (r - l) / 2;
            self.seek_to_index(mid);

            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => l = mid + 1,
                std::cmp::Ordering::Equal => {
                    tracing::debug!("seek to key: {:?}, hit!", key);
                    return;
                }
                std::cmp::Ordering::Greater => r = mid,
            }
        }

        self.seek_to_index(l);
        tracing::debug!("seek to key: {:?}, not hit……", key);
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;

    use crate::{
        format::{
            key::{KeyBytes, Seq},
            value::Value,
        },
        table::block::BlockBuilder,
        test_utils::{gen_key_value, init_tracing_not_failed},
    };

    #[test]
    fn test_block_iter() -> anyhow::Result<()> {
        init_tracing_not_failed();

        const COUNT: usize = 100;

        let items = (0..COUNT).map(|n| gen_key_value(n as u64, n)).collect_vec();
        let mut block_builder = BlockBuilder::new();
        for (k, v) in items.iter() {
            block_builder.add_entry(k.clone(), v.clone());
        }

        let block = block_builder.finish();
        let mut block_iter = block.iter();
        let mut results = Vec::with_capacity(COUNT);
        while block_iter.is_valid() {
            let key = block_iter.key().into_key_bytes();
            let value = block_iter.value();
            results.push((key, value));
            block_iter.next();
        }

        assert_eq!(items.len(), results.len());
        for (expected, actual) in items.iter().zip(results.iter()) {
            assert_eq!(expected, actual);
        }

        Ok(())
    }

    #[test]
    fn test_block_seek() -> anyhow::Result<()> {
        init_tracing_not_failed();

        let items = vec![
            gen_key_value(10, 10),
            gen_key_value(11, 11),
            gen_key_value(15, 15),
            gen_key_value(16, 16),
        ];
        let mut builder = BlockBuilder::new();
        for (k, v) in items.iter() {
            builder.add_entry(k.clone(), v.clone());
        }

        let block = builder.finish();
        let mut iter = block.iter();

        let mut result = Vec::new();
        while iter.is_valid() {
            result.push((iter.key().into_key_bytes(), iter.value()));
            iter.next();
        }

        assert_eq!(items.len(), result.len());
        for (expected, actual) in items.iter().zip(result.iter()) {
            assert_eq!(expected, actual);
        }

        iter.seek_to_key(gen_key_value(0, 0).0.as_key_slice());
        assert_eq!(
            gen_key_value(10, 10),
            (iter.key().into_key_bytes(), iter.value())
        );

        iter.seek_to_key(gen_key_value(10, 10).0.as_key_slice());
        assert_eq!(
            gen_key_value(10, 10),
            (iter.key().into_key_bytes(), iter.value())
        );

        iter.seek_to_key(gen_key_value(11, 11).0.as_key_slice());
        assert_eq!(
            gen_key_value(11, 11),
            (iter.key().into_key_bytes(), iter.value())
        );

        iter.seek_to_key(gen_key_value(12, 12).0.as_key_slice());
        assert_eq!(
            gen_key_value(15, 15),
            (iter.key().into_key_bytes(), iter.value())
        );

        iter.seek_to_key(gen_key_value(100, 100).0.as_key_slice());
        assert!(!iter.is_valid());

        Ok(())
    }
}
