#![allow(unused)]

use std::{mem::transmute, sync::Arc};

use bytes::Bytes;
use fast_async_mutex::mutex::Mutex;
use remdb_skiplist::{
    comparator::prelude::DefaultComparator, mem_allocator::prelude::BlockArena, skip_list::SkipList,
};

use crate::{
    error::Result,
    key::{KeyBytes, KeySlice},
    value_log::ValueLog,
};

pub struct MemTable {
    list: Arc<SkipList<KeyBytes, Bytes, DefaultComparator<KeyBytes>, Arc<BlockArena>>>,
    id: usize,
    wal: Option<Arc<Mutex<ValueLog>>>,
}

impl Clone for MemTable {
    fn clone(&self) -> Self {
        Self {
            list: self.list.clone(),
            id: self.id,
            wal: self.wal.clone(),
        }
    }
}

impl MemTable {
    pub fn new(wal: Option<ValueLog>, id: usize) -> Self {
        let list = Arc::new(SkipList::new(
            DefaultComparator::default(),
            Arc::new(BlockArena::default()),
        ));
        let wal = wal.map(|wal| Arc::new(Mutex::new(wal)));
        Self { list, id, wal }
    }

    pub async fn put(&self, key: KeySlice<'_>, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)]).await?;
        Ok(())
    }

    pub async fn put_batch<'a>(&self, data: &[(KeySlice<'a>, &'a [u8])]) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.lock().await.put_batch(data).await?;
        }

        for (k, v) in data.iter() {
            self.list
                .insert(k.clone().into_key_bytes(), Bytes::copy_from_slice(v));
        }

        Ok(())
    }

    pub async fn get(&self, key: KeySlice<'_>) -> Result<Option<Bytes>> {
        let key = KeyBytes::new(
            Bytes::from_static(unsafe { transmute(key.key()) }),
            key.seq(),
        );

        let mut iter = self.list.iter();
        iter.seek(&key);

        if iter.is_valid() && iter.key().unwrap().cmp(&key).is_eq() {
            return Ok(Some(iter.value().map(|v| v.clone()).expect("WTF?")));
        }
        Ok(None)
    }

    pub fn iter(&self) -> MemTableIter {
        MemTableIter::new(self.clone())
    }
}

pub struct MemTableIter {
    memtable: MemTable,
    iter: remdb_skiplist::skip_list::SkipListIter<
        KeyBytes,
        Bytes,
        DefaultComparator<KeyBytes>,
        Arc<BlockArena>,
    >,
}

impl MemTableIter {
    fn new(mem: MemTable) -> Self {
        let mut iter = mem.list.iter();
        iter.seek_to_first();
        Self {
            memtable: mem,
            iter,
        }
    }
}

impl crate::iterator::Iterator for MemTableIter {
    type KeyType<'a> = KeySlice<'a>;

    async fn key(&self) -> Self::KeyType<'_> {
        assert!(self.iter.is_valid());
        self.iter.key().as_ref().unwrap().as_key_slice()
    }

    async fn value(&self) -> &[u8] {
        assert!(self.iter.is_valid());
        &self.iter.value().as_ref().unwrap()[..]
    }

    async fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    async fn rewind(&mut self) -> Result<()> {
        self.iter.seek_to_first();
        Ok(())
    }

    async fn next(&mut self) -> Result<()> {
        self.iter.next();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::{iterator::Iterator, key::KeySlice, memtable::MemTable};

    fn gen_test_data(count: usize) -> Vec<(String, String)> {
        (0..count)
            .map(|i| (format!("key{:09}", i), format!("value{:09}", i)))
            .collect_vec()
    }

    #[tokio::test]
    async fn test_get_and_set() {
        let data = gen_test_data(10000);
        let mem = MemTable::new(None, 0);

        for (i, (k, v)) in data.iter().enumerate() {
            let key = KeySlice::new(k.as_bytes(), i as _);
            mem.put(key, v.as_bytes()).await.expect("put not failed");
        }

        for (i, (k, v)) in data.iter().enumerate() {
            let key = KeySlice::new(k.as_bytes(), i as _);
            let value = mem
                .get(key)
                .await
                .expect("get not failed")
                .expect("value not found");
            assert_eq!(value, v.as_bytes());
        }
    }

    #[tokio::test]
    async fn test_mem_iter() {
        let data = gen_test_data(1000);
        let mem = MemTable::new(None, 0);

        for (i, (k, v)) in data.iter().enumerate() {
            let key = KeySlice::new(k.as_bytes(), i as _);
            mem.put(key, v.as_bytes()).await.expect("put not failed");
        }

        let mut iter = mem.iter();
        for (i, (k, v)) in data.iter().enumerate() {
            assert!(iter.is_valid().await);
            let key = iter.key().await;
            let value = iter.value().await;
            assert_eq!(key.key(), k.as_bytes());
            assert_eq!(key.seq(), i as _);
            assert_eq!(value, v.as_bytes());
            iter.next().await.expect("next not failed");
        }
    }
}
