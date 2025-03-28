#![allow(unused)]

use std::{mem::transmute, ops::Bound, sync::Arc};

use bytes::Bytes;
use fast_async_mutex::mutex::Mutex;
use itertools::Itertools;
use remdb_skiplist::{
    comparator::prelude::DefaultComparator, mem_allocator::prelude::BlockArena, skip_list::SkipList,
};

use crate::{
    error::Result,
    format::{
        key::{KeyBytes, KeySlice},
        value::Value,
    },
    iterator::Iter,
    table::table_builder::TableBuilder,
    value_log::{Entry, ValueLogFile},
};

pub struct MemTable {
    list: Arc<SkipList<KeyBytes, Bytes, DefaultComparator<KeyBytes>, Arc<BlockArena>>>,
    id: u32,
    wal: Option<Arc<Mutex<ValueLogFile>>>,
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
    pub fn new(wal: Option<Arc<Mutex<ValueLogFile>>>, id: u32) -> Self {
        let list = Arc::new(SkipList::new(
            DefaultComparator::default(),
            Arc::new(BlockArena::default()),
        ));
        Self { list, id, wal }
    }

    pub async fn put(&self, key: KeyBytes, value: Bytes) -> Result<()> {
        self.put_batch(&[(key, value)]).await?;
        Ok(())
    }

    pub async fn put_batch(&self, data: &[(KeyBytes, Bytes)]) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.lock()
                .await
                .put_batch(
                    &data
                        .iter()
                        .map(|(k, v)| Entry::new(k.seq(), k.real_key.clone(), v.clone()))
                        .collect_vec(),
                )
                .await?;
        }

        for (k, v) in data.iter() {
            self.list.insert(k.clone(), v.clone());
        }

        Ok(())
    }

    pub async fn get(&self, key: KeySlice<'_>) -> Result<Option<Bytes>> {
        let key = KeyBytes::new(
            Bytes::from_static(unsafe { transmute::<&[u8], &[u8]>(key.key()) }),
            key.seq(),
        );

        let mut iter = self.list.iter();
        iter.seek(&key);

        if iter.is_valid() && iter.key().unwrap().cmp(&key).is_eq() {
            return Ok(Some(iter.value().cloned().expect("WTF?")));
        }
        Ok(None)
    }

    pub async fn scan(
        &self,
        lower: Bound<KeySlice<'_>>,
        upper: Bound<KeySlice<'_>>,
    ) -> MemTableIter {
        let lower = map_bound(lower);
        let upper = map_bound(upper);

        MemTableIter::new(self.clone(), lower, upper)
    }

    /// create a memtable iter, and seek to first.
    pub fn iter(&self) -> MemTableIter {
        MemTableIter::new(self.clone(), Bound::Unbounded, Bound::Unbounded)
    }

    pub fn memory_usage(&self) -> usize {
        self.list.mem_usage()
    }

    pub fn id(&self) -> u32 {
        self.id
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
    bound: (Bound<KeyBytes>, Bound<KeyBytes>),
}

impl MemTableIter {
    /// create a new iter, and seek to the lower key
    fn new(mem: MemTable, lower: Bound<KeyBytes>, upper: Bound<KeyBytes>) -> Self {
        let mut iter = mem.list.iter();

        match &lower {
            Bound::Included(key) => iter.seek(key),
            Bound::Excluded(key) => {
                iter.seek(key);
                if iter.is_valid() {
                    iter.next();
                }
            }
            Bound::Unbounded => iter.seek_to_first(),
        }

        Self {
            memtable: mem,
            iter,
            bound: (lower, upper),
        }
    }

    pub(crate) fn raw_key(&self) -> KeyBytes {
        self.iter.key().expect("should be valid").clone()
    }

    pub(crate) fn raw_value(&self) -> Bytes {
        self.iter.value().expect("should be valid").clone()
    }
}

impl crate::iterator::Iter for MemTableIter {
    async fn key(&self) -> KeyBytes {
        assert!(self.iter.is_valid());
        self.iter.key().unwrap().clone()
    }

    async fn value(&self) -> Value {
        assert!(self.iter.is_valid());
        Value::from_raw_value(self.iter.value().unwrap().clone())
    }

    async fn is_valid(&self) -> bool {
        if !self.iter.is_valid() {
            return false;
        }

        match &self.bound.1 {
            Bound::Included(key) => {
                // dbg!(self.iter.key().unwrap().cmp(key).is_le());
                self.iter.key().unwrap().cmp(key).is_le()
            }
            Bound::Excluded(key) => {
                // dbg!(self.iter.key().unwrap().cmp(key).is_lt());
                self.iter.key().unwrap().cmp(key).is_lt()
            }
            Bound::Unbounded => true,
        }
    }

    // async fn rewind(&mut self) -> Result<()> {
    //     match &self.bound.0 {
    //         Bound::Included(key) => self.iter.seek(key),
    //         Bound::Excluded(key) => {
    //             self.iter.seek(key);
    //             if self.iter.is_valid() {
    //                 self.iter.next();
    //             }
    //         }
    //         Bound::Unbounded => self.iter.seek_to_first(),
    //     }
    //     Ok(())
    // }

    async fn next(&mut self) -> Result<()> {
        assert!(self.is_valid().await);
        self.iter.next();
        Ok(())
    }
}

fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    bound.map(|key| key.into_key_bytes())
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use bytes::Bytes;
    use itertools::Itertools;

    use crate::{
        format::key::{KeyBytes, KeySlice, Seq},
        iterator::Iter,
        memtable::MemTable,
        mvcc::{TS_BEGIN, TS_END},
    };

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
            let key = KeyBytes::new(Bytes::copy_from_slice(k.as_bytes()), i as _);
            let value = Bytes::copy_from_slice(v.as_bytes());
            mem.put(key, value).await.expect("put not failed");
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
            let key = KeyBytes::new(Bytes::copy_from_slice(k.as_bytes()), i as _);
            let value = Bytes::copy_from_slice(v.as_bytes());
            mem.put(key, value).await.expect("put not failed");
        }

        let mut iter = mem.iter();
        for (i, (k, v)) in data.iter().enumerate() {
            assert!(iter.is_valid().await);
            let key = iter.key().await;
            let value = iter.value().await;
            assert_eq!(key.key(), k.as_bytes());
            assert_eq!(key.seq(), i as _);
            assert_eq!(value.value_or_ptr, v.as_bytes());
            iter.next().await.expect("next not failed");
        }
    }

    #[tokio::test]
    async fn test_mem_range() {
        let data = gen_test_data(100);

        let mem = MemTable::new(None, 0);
        for (i, (k, v)) in data.iter().enumerate() {
            let key = KeyBytes::new(Bytes::copy_from_slice(k.as_bytes()), i as _);
            let value = Bytes::copy_from_slice(v.as_bytes());
            mem.put(key, value).await.expect("put not failed");
        }

        let mut iter = mem.scan(Bound::Unbounded, Bound::Unbounded).await;
        for (i, (k, v)) in data.iter().enumerate() {
            assert!(iter.is_valid().await);
            let key = iter.key().await;
            let value = iter.value().await;
            assert_eq!(key.key(), k.as_bytes());
            assert_eq!(key.seq(), i as _);
            assert_eq!(value.value_or_ptr, v.as_bytes());
            iter.next().await.expect("next not failed");
        }

        let upper = format!("key{:09}", 10);
        let mut iter = mem
            .scan(
                Bound::Unbounded,
                Bound::Included(KeySlice::new(upper.as_bytes(), Seq::MIN)),
            )
            .await;
        for (i, (k, v)) in data.iter().take(11).enumerate() {
            assert!(iter.is_valid().await);
            let key = iter.key().await;
            let value = iter.value().await;
            assert_eq!(key.key(), k.as_bytes());
            assert_eq!(key.seq(), i as _);
            assert_eq!(value.value_or_ptr, v.as_bytes());
            iter.next().await.expect("next not failed");
        }

        let lower = format!("key{:09}", 10);
        let mut iter = mem
            .scan(
                Bound::Included(KeySlice::new(lower.as_bytes(), Seq::MAX)),
                Bound::Unbounded,
            )
            .await;
        for (i, (k, v)) in data.iter().skip(10).enumerate() {
            assert!(iter.is_valid().await);
            let key = iter.key().await;
            let value = iter.value().await;

            // dbg!(&key, &value, k, v);

            assert_eq!(key.key(), k.as_bytes());
            assert_eq!(key.seq(), i as u64 + 10);
            assert_eq!(value.value_or_ptr, v.as_bytes());
            iter.next().await.expect("next not failed");
        }

        let lower = format!("key{:09}", 10);
        let upper = format!("key{:09}", 200);
        let mut iter = mem
            .scan(
                Bound::Included(KeySlice::new(lower.as_bytes(), Seq::MAX)),
                Bound::Excluded(KeySlice::new(upper.as_bytes(), Seq::MIN)),
            )
            .await;

        for (i, (k, v)) in data.iter().skip(10).take(190).enumerate() {
            assert!(iter.is_valid().await);
            let key = iter.key().await;
            let value = iter.value().await;
            assert_eq!(key.key(), k.as_bytes());
            assert_eq!(key.seq(), i as u64 + 10);
            assert_eq!(value.value_or_ptr, v.as_bytes());
            iter.next().await.expect("next not failed");
        }
    }

    #[tokio::test]
    async fn test_time_get() {
        let data = [
            (1, "key1", "value1"),
            (2, "key1", "value2"),
            (3, "key1", "value3"),
            (4, "key1", "value4"),
            (5, "key1", "value5"),
        ];

        let mem = MemTable::new(None, 0);
        for (seq, k, v) in data.iter() {
            let key = KeyBytes::new(Bytes::copy_from_slice(k.as_bytes()), *seq);
            let value = Bytes::copy_from_slice(v.as_bytes());
            mem.put(key, value).await.expect("put not failed");
        }

        for (seq, k, v) in data.iter() {
            let key = KeySlice::new(k.as_bytes(), *seq);
            let iter = mem
                .scan(
                    Bound::Included(key),
                    Bound::Included(KeySlice::new("key1".as_bytes(), TS_END)),
                )
                .await;

            assert!(iter.is_valid().await);
            assert_eq!(iter.key().await.as_key_slice(), key);
            assert_eq!(iter.value().await.value_or_ptr, v.as_bytes());
        }

        // later
        let key = KeySlice::new("key1".as_bytes(), TS_BEGIN);
        let iter = mem
            .scan(
                Bound::Included(key),
                Bound::Included(KeySlice::new("key1".as_bytes(), TS_END)),
            )
            .await;
        eprintln!("key: {:?}", iter.iter.key());

        assert!(iter.is_valid().await);
        assert_eq!(
            iter.key().await.as_key_slice(),
            KeySlice::new("key1".as_bytes(), 5)
        );
        assert_eq!(iter.value().await.value_or_ptr, "value5".as_bytes());
    }
}
