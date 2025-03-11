#![allow(unused)]

use std::{cell::RefCell, mem::transmute, path::Path, sync::Arc};

use bytes::{Buf, Bytes};
use parking_lot::Mutex;
use remdb_skiplist::{
    comparator::Comparator, mem_allocator::prelude::BlockArena, skip_list::SkipList,
};

use crate::{db_options::DBOptions, error::Result, wal::mmap_impl::Wal};

#[derive(Debug, Clone, Default)]
struct KeyComparator;

impl remdb_skiplist::comparator::Comparator for KeyComparator {
    type Item = (Bytes, u64);

    fn compare(&self, a: &Self::Item, b: &Self::Item) -> std::cmp::Ordering {
        a.0.cmp(&b.0).then(a.1.cmp(&b.1).reverse())
    }
}

#[derive(Clone)]
pub struct MemTable {
    list: Arc<SkipList<(Bytes, u64), Bytes, KeyComparator, BlockArena>>,
    wal: Option<Arc<Mutex<Wal>>>,
    id: usize,
}

impl MemTable {
    pub fn create(
        id: usize,
        path: Option<impl AsRef<Path>>,
        options: &Arc<DBOptions>,
    ) -> Result<Self> {
        let wal = if let Some(p) = path {
            let wal = Wal::open(p, options.clone())?;
            Some(Arc::new(Mutex::new(wal)))
        } else {
            None
        };

        Ok(Self {
            id,
            list: Arc::new(SkipList::new(
                KeyComparator::default(),
                BlockArena::default(),
            )),
            wal,
        })
    }

    pub fn get(&self, seq: u64, key: &[u8]) -> Option<Bytes> {
        let key = unsafe { Bytes::from_static(transmute(key)) };
        let key = (key, seq);

        let mut iter = self.list.iter();
        iter.seek(&key);

        if let Some(ikey) = iter.key()
            && KeyComparator::default().compare(&key, ikey).is_eq()
        {
            Some(iter.value().unwrap().clone())
        } else {
            None
        }
    }

    pub fn put(&self, seq: u64, key: &[u8], value: &[u8]) -> Result<()> {
        self.list.insert(
            (Bytes::copy_from_slice(key), seq),
            Bytes::copy_from_slice(value),
        );

        if let Some(wal) = &self.wal {
            let mut wal = wal.lock();
            wal.put(seq, key, value)?;
            wal.sync()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::db_options::DBOpenOptions;

    use super::MemTable;

    #[test]
    fn put_some_data() -> anyhow::Result<()> {
        let mem = MemTable::create(0, None::<PathBuf>, &DBOpenOptions::default().build())?;

        mem.put(1, b"key1", b"value1")?;
        mem.put(2, b"key2", b"value2")?;
        mem.put(3, b"key1", b"value3")?;

        assert_eq!(mem.get(3, b"key1").unwrap(), b"value3".as_ref());
        assert_eq!(mem.get(2, b"key2").unwrap(), b"value2".as_ref());

        mem.put(4, b"key2", b"value4")?;
        mem.put(5, b"key5", b"value5")?;

        assert_eq!(mem.get(4, b"key1"), None);
        assert_eq!(mem.get(4, b"key2").unwrap(), b"value4".as_ref());
        assert_eq!(mem.get(5, b"key5").unwrap(), b"value5".as_ref());

        Ok(())
    }
}
