use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use remdb_skiplist::{
    mem_allocator::prelude::*,
    skip_list::{SkipList, SkipListIter},
};

use crate::{comparator::Comparator, format::make_internal_key, iterator::Iter};

#[derive(Clone)]
struct SkipListComparator<C: Comparator> {
    c: C,
}

impl<C> SkipListComparator<C>
where
    C: Comparator,
{
    pub fn new(c: C) -> Self {
        Self { c }
    }
}

impl<C> Default for SkipListComparator<C>
where
    C: Comparator + Default,
{
    fn default() -> Self {
        Self::new(C::default())
    }
}

impl<C> remdb_skiplist::comparator::Comparator for SkipListComparator<C>
where
    C: Comparator,
{
    type Item = Bytes;

    fn compare(&self, a: &Self::Item, b: &Self::Item) -> std::cmp::Ordering {
        self.c.compare(a.as_ref(), b.as_ref())
    }
}

pub struct MemTable<C>
where
    C: Comparator,
{
    c: C,
    list: Arc<SkipList<Bytes, Bytes, SkipListComparator<C>, DefaultAllocator>>,
}

// ????
impl<C> Clone for MemTable<C>
where
    C: Comparator,
{
    fn clone(&self) -> Self {
        Self {
            c: self.c.clone(),
            list: self.list.clone(),
        }
    }
}

impl<C> MemTable<C>
where
    C: Comparator,
{
    pub fn new(c: C) -> Self {
        let allocator = DefaultAllocator::default();
        let inner_cmp = SkipListComparator::new(c.clone());
        let list = Arc::new(SkipList::new(inner_cmp, allocator));
        Self { c, list }
    }

    pub fn mem_usage(&self) -> usize {
        self.list.mem_usage()
    }

    pub fn put(&self, seq: u64, key: &[u8], value: &[u8]) {
        let inner_key = make_internal_key(key, seq);
        let value = Bytes::copy_from_slice(value);
        self.list.insert(inner_key, value);
    }

    pub fn get(&self, seq: u64, key: &[u8]) -> Option<Bytes> {
        todo!()
    }

    pub fn iter(&self) -> MemTableIter<C> {
        MemTableIter::new(self)
    }
}

pub struct MemTableIter<C>
where
    C: Comparator,
{
    iter: SkipListIter<Bytes, Bytes, SkipListComparator<C>, DefaultAllocator>,
    buf: BytesMut,
}

impl<C> MemTableIter<C>
where
    C: Comparator,
{
    pub fn new(table: &MemTable<C>) -> Self {
        Self {
            iter: table.list.iter(),
            buf: BytesMut::new(),
        }
    }
}

impl<C> Iter for MemTableIter<C>
where
    C: Comparator,
{
    fn prev(&mut self) {
        todo!()
    }

    fn next(&mut self) {
        todo!()
    }

    fn key(&self) -> Option<&[u8]> {
        todo!()
    }

    fn value(&self) -> Option<&[u8]> {
        todo!()
    }

    fn rewind(&mut self, from_last: bool) {
        todo!()
    }

    fn seek(&mut self, key: &[u8]) {
        todo!()
    }

    fn is_valid(&self) -> bool {
        todo!()
    }

    fn status(&mut self) -> crate::error::Result<()> {
        todo!()
    }
}
