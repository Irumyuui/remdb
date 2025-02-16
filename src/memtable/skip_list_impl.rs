use std::{
    mem::transmute,
    ops::Bound,
    sync::{atomic::AtomicUsize, Arc},
};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::{db::format::FileId, error::Result, iter::Iter, options::Options};

use super::format::ImplMemTable;

struct MemTableInner {
    skip_list: SkipMap<Bytes, Bytes>,
    mem_usage: AtomicUsize,
}

#[derive(Clone)]
pub struct MemTable {
    inner: Arc<MemTableInner>,
    id: FileId,
}

impl ImplMemTable for MemTable {
    type IterType = MemTableIter;

    fn create(id: FileId, _options: Arc<Options>) -> Self {
        Self {
            inner: Arc::new(MemTableInner {
                skip_list: SkipMap::new(),
                mem_usage: AtomicUsize::new(0),
            }),
            id,
        }
    }

    fn get(&self, key: &[u8]) -> Option<Bytes> {
        let key = Bytes::from_static(unsafe { transmute(key) });
        self.inner.skip_list.get(&key).map(|v| v.value().clone())
    }

    fn put(&self, key: Bytes, value: Bytes) {
        let inner = self.inner.as_ref();
        let mem_usage = (key.len() + value.len()) * std::mem::size_of::<u8>()
            + inner.mem_usage.load(std::sync::atomic::Ordering::Relaxed);
        inner.skip_list.insert(key, value);
        inner
            .mem_usage
            .fetch_add(mem_usage, std::sync::atomic::Ordering::Relaxed);
    }

    fn approximate_size(&self) -> usize {
        self.inner
            .mem_usage
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn iter(&self, start: Bound<&[u8]>, limit: Bound<&[u8]>) -> Self::IterType {
        let start = map_bound(start);
        let limit = map_bound(limit);
        let table = self.inner.clone();

        let mut iter = MemTableIterBuilder {
            table,
            iter_builder: |table| table.skip_list.range((start, limit)),
            item: None,
        }
        .build();
        iter.next().unwrap();
        iter
    }

    fn id(&self) -> &FileId {
        &self.id
    }
}

#[self_referencing]
pub struct MemTableIter {
    table: Arc<MemTableInner>,

    #[borrows(table)]
    #[not_covariant]
    iter: crossbeam_skiplist::map::Range<'this, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>,

    item: Option<(Bytes, Bytes)>,
}

impl Iter for MemTableIter {
    fn is_valid(&self) -> bool {
        self.borrow_item().is_some()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().as_ref().unwrap().0.as_ref()
    }

    fn value(&self) -> &[u8] {
        self.borrow_item().as_ref().unwrap().1.as_ref()
    }

    fn next(&mut self) -> Result<()> {
        self.with_mut(|this| {
            let entry = this.iter.next();
            let item = match entry {
                Some(e) => Some((e.key().clone(), e.value().clone())),
                None => None,
            };
            *this.item = item;
        });

        Ok(())
    }
}

fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    bound.map(|b| Bytes::copy_from_slice(b))
}
