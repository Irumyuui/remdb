#![allow(unused)]

use std::{mem::transmute, ops::Bound, sync::Arc};

use bytes::Bytes;
use fast_async_mutex::mutex::Mutex;
use itertools::Itertools;
use remdb_skiplist::{
    comparator::prelude::DefaultComparator, mem_allocator::prelude::BlockArena, skip_list::SkipList,
};

use crate::{
    error::KvResult,
    format::{
        key::{KeyBytes, KeySlice},
        value::Value,
    },
    kv_iter::{KvItem, KvIter, Peekable},
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

    pub async fn put(&self, key: KeyBytes, value: Bytes) -> KvResult<()> {
        self.put_batch(&[(key, value)]).await?;
        Ok(())
    }

    pub async fn put_batch(&self, data: &[(KeyBytes, Bytes)]) -> KvResult<()> {
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

    pub async fn get(&self, key: KeySlice<'_>) -> KvResult<Option<Bytes>> {
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

    pub async fn scan(&self, lower: Bound<KeyBytes>, upper: Bound<KeyBytes>) -> MemTableIter {
        // let lower = map_bound(lower);
        // let upper = map_bound(upper);

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

    current: Option<KvItem>,
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

        let current = if iter.is_valid() {
            let key = iter.key().unwrap().clone();
            let value = iter.value().unwrap().clone();
            Some(KvItem {
                key: key.clone(),
                value: Value::from_raw_value(value),
            })
        } else {
            None
        };

        Self {
            memtable: mem,
            iter,
            bound: (lower, upper),
            current,
        }
    }

    pub(crate) fn raw_key(&self) -> KeyBytes {
        self.iter.key().expect("should be valid").clone()
    }

    pub(crate) fn raw_value(&self) -> Bytes {
        self.iter.value().expect("should be valid").clone()
    }
}

fn map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    bound.map(|key| key.into_key_bytes())
}

impl KvIter for MemTableIter {
    async fn next(&mut self) -> KvResult<Option<KvItem>> {
        let current = match self.current.take() {
            Some(item) => item,
            None => return Ok(None),
        };

        self.iter.next();
        if self.iter.is_valid() {
            let key = self.iter.key().unwrap().clone();
            let value = self.iter.value().unwrap().clone();
            self.current = Some(KvItem {
                key: key.clone(),
                value: Value::from_raw_value(value),
            });
        }

        Ok(Some(current))
    }
}

impl Peekable for MemTableIter {
    fn peek(&self) -> Option<&KvItem> {
        self.current.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use bytes::Bytes;
    use itertools::Itertools;

    use crate::{
        format::key::{KeyBytes, KeySlice, Seq},
        // iterator::Iter,
        memtable::MemTable,
        mvcc::{TS_BEGIN, TS_END},
    };

    pub(crate) fn gen_test_data(count: usize) -> Vec<(String, String)> {
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
}

/// For `iter2`
#[cfg(test)]
mod tests2 {
    use std::ops::Bound;

    use bytes::Bytes;

    use crate::{
        format::key::{KeyBytes, KeySlice, Seq},
        kv_iter::KvIter,
        memtable::{MemTable, tests::gen_test_data},
        mvcc::{TS_BEGIN, TS_END},
    };

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
            let item = iter.next().await.expect("next not failed").unwrap();
            assert_eq!(item.key.key(), k.as_bytes());
            assert_eq!(item.key.seq(), i as _);
            assert_eq!(item.value.as_raw_value(), v.as_bytes());
            assert_eq!(item.value.as_raw_value(), v.as_bytes());
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
            let item = iter.next().await.expect("next not failed").unwrap();
            assert_eq!(item.key.key(), k.as_bytes(),);
            assert_eq!(item.key.seq(), i as _);
            assert_eq!(item.value.as_raw_value(), v.as_bytes());
        }

        let upper = format!("key{:09}", 10);
        let mut iter = mem
            .scan(
                Bound::Unbounded,
                Bound::Included(KeySlice::new(upper.as_bytes(), Seq::MIN).into_key_bytes()),
            )
            .await;
        for (i, (k, v)) in data.iter().take(11).enumerate() {
            let item = iter.next().await.expect("next not failed").unwrap();
            assert_eq!(item.key.key(), k.as_bytes());
            assert_eq!(item.key.seq(), i as _);
            assert_eq!(item.value.as_raw_value(), v.as_bytes());
        }

        let lower = format!("key{:09}", 10);
        let mut iter = mem
            .scan(
                Bound::Included(KeySlice::new(lower.as_bytes(), Seq::MAX).into_key_bytes()),
                Bound::Unbounded,
            )
            .await;
        for (i, (k, v)) in data.iter().skip(10).enumerate() {
            let item = iter.next().await.expect("next not failed").unwrap();
            assert_eq!(item.key.key(), k.as_bytes());
            assert_eq!(item.key.seq(), i as u64 + 10);
            assert_eq!(item.value.as_raw_value(), v.as_bytes());
        }

        let lower = format!("key{:09}", 10);
        let upper = format!("key{:09}", 200);
        let mut iter = mem
            .scan(
                Bound::Included(KeySlice::new(lower.as_bytes(), Seq::MAX).into_key_bytes()),
                Bound::Excluded(KeySlice::new(upper.as_bytes(), Seq::MIN).into_key_bytes()),
            )
            .await;

        for (i, (k, v)) in data.iter().skip(10).take(190).enumerate() {
            let item = iter.next().await.expect("next not failed").unwrap();
            assert_eq!(item.key.key(), k.as_bytes());
            assert_eq!(item.key.seq(), i as u64 + 10);
            assert_eq!(item.value.as_raw_value(), v.as_bytes());
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
            let key = KeySlice::new(k.as_bytes(), *seq).into_key_bytes();
            let mut iter = mem
                .scan(
                    Bound::Included(key),
                    Bound::Included(KeySlice::new("key1".as_bytes(), TS_END).into_key_bytes()),
                )
                .await;

            let item = iter.next().await.expect("next not failed").unwrap();
            assert_eq!(item.key.key(), k.as_bytes());
            assert_eq!(item.key.seq(), *seq);
            assert_eq!(item.value.as_raw_value(), v.as_bytes());
        }

        // later
        let key = KeySlice::new("key1".as_bytes(), TS_BEGIN).into_key_bytes();
        let mut iter = mem
            .scan(
                Bound::Included(key),
                Bound::Included(KeySlice::new("key1".as_bytes(), TS_END).into_key_bytes()),
            )
            .await;

        let item = iter.next().await.expect("next not failed").unwrap();
        assert_eq!(item.key.key(), "key1".as_bytes());
        assert_eq!(item.key.seq(), 5);
        assert_eq!(item.value.as_raw_value(), "value5".as_bytes());
    }
}
