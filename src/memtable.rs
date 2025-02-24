#![allow(unused)]

use std::{mem::transmute, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};
use remdb_skiplist::{
    mem_allocator::prelude::DefaultAllocator,
    skip_list::{SkipList, SkipListIter},
};

use crate::{
    comparator::prelude::*,
    format::{
        get_mem_internal_key, get_mem_value, get_mem_value_to_bytes, make_lookup_key,
        make_memtable_key,
    },
    iterator::Iter,
};

#[derive(Clone)]
pub struct MemTableKeyComparator<C>
where
    C: Comparator,
{
    c: InternalKeyComparator<C>,
}

impl<C> Comparator for MemTableKeyComparator<C>
where
    C: Comparator,
{
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        let a_ikey = get_mem_internal_key(a);
        let b_ikey = get_mem_internal_key(b);
        self.c.compare(a_ikey, b_ikey)
    }
}

impl<C> remdb_skiplist::comparator::Comparator for MemTableKeyComparator<C>
where
    C: Comparator,
{
    type Item = Bytes;

    fn compare(&self, a: &Self::Item, b: &Self::Item) -> std::cmp::Ordering {
        Comparator::compare(self, &a, &b)
    }
}

type Allocator = DefaultAllocator;
type List<C> = Arc<SkipList<Bytes, (), MemTableKeyComparator<C>, Allocator>>;
type ListIter<C> = SkipListIter<Bytes, (), MemTableKeyComparator<C>, Allocator>;

pub struct MemTable<C>
where
    C: Comparator,
{
    c: MemTableKeyComparator<C>,
    list: List<C>,
}

impl<C: Comparator> MemTable<C> {
    pub fn new(c: InternalKeyComparator<C>) -> Self {
        let allocator = Allocator::default();
        let c = MemTableKeyComparator { c };
        let list = Arc::new(SkipList::new(c.clone(), allocator));
        Self { c, list }
    }

    pub fn put(&self, seq: u64, key: &[u8], value: &[u8]) {
        let key = make_memtable_key(seq, key, value);
        self.list.insert(key, ());
    }

    pub fn get(&self, seq: u64, key: &[u8]) -> Option<Bytes> {
        let lookup_key = make_lookup_key(seq, key);
        let mut iter = self.iter();
        iter.seek(&lookup_key);
        if let Some(internal_key) = iter.key()
            && self
                .c
                .c
                .c
                .compare(internal_key[..internal_key.len() - 8].as_ref(), key)
                .is_eq()
        {
            let raw_key = iter.iter.key().unwrap(); // must have
            Some(get_mem_value_to_bytes(raw_key))
        } else {
            None
        }
    }

    pub fn iter(&self) -> MemTableIter<C> {
        MemTableIter::new(self)
    }

    pub fn mem_usage(&self) -> usize {
        self.list.mem_usage()
    }
}

pub struct MemTableIter<C: Comparator> {
    iter: ListIter<C>,
}

impl<C> MemTableIter<C>
where
    C: Comparator,
{
    pub fn new(table: &MemTable<C>) -> Self {
        Self {
            iter: table.list.iter(),
        }
    }
}

impl<C: Comparator> Iter for MemTableIter<C> {
    fn prev(&mut self) {
        self.iter.prev();
    }

    fn next(&mut self) {
        self.iter.next();
    }

    fn key(&self) -> Option<&[u8]> {
        self.iter
            .key()
            .map(|key| key[..].as_ref())
            .map(get_mem_internal_key)
    }

    fn value(&self) -> Option<&[u8]> {
        self.iter
            .key()
            .map(|key| key[..].as_ref())
            .map(get_mem_value)
    }

    fn rewind(&mut self, from_last: bool) {
        match from_last {
            false => self.iter.seek_to_first(),
            true => self.iter.seek_to_last(),
        }
    }

    fn seek(&mut self, key: &[u8]) {
        let seek_key = Bytes::from_static(unsafe { transmute(key) });
        self.iter.seek(&seek_key);
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn status(&mut self) -> crate::error::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        comparator::prelude::{BytewiseComparator, InternalKeyComparator},
        format::get_user_key,
        iterator::Iter,
    };

    use super::MemTable;

    #[test]
    fn add_some_and_get() {
        let table = MemTable::new(InternalKeyComparator::new(BytewiseComparator::default()));

        table.put(4, b"key1", b"value3");
        table.put(2, b"key1", b"value2");
        table.put(3, b"key1", b"");
        table.put(1, b"key1", b"value1");
        table.put(5, b"key2", b"value4");

        let res = table.get(114514, b"is nothing");
        assert!(res.is_none());

        let res = table.get(0, b"key1");
        assert!(res.is_none()); // should not peek

        let res = table.get(1, b"key1");
        assert_eq!(res.unwrap()[..].as_ref(), b"value1");

        let res = table.get(2, b"key1");
        assert_eq!(res.unwrap()[..].as_ref(), b"value2");
        let res = table.get(2, b"key1");

        let res = table.get(3, b"key1");
        assert_eq!(res.unwrap()[..].as_ref(), b"");

        let res = table.get(5, b"key1");
        assert_eq!(res.unwrap()[..].as_ref(), b"value3");

        let res = table.get(5, b"key2");
        assert_eq!(res.unwrap()[..].as_ref(), b"value4");
    }

    #[test]
    fn memtable_iter() {
        let table = MemTable::new(InternalKeyComparator::new(BytewiseComparator::default()));

        let cases: Vec<(u64, &[u8], &[u8])> = vec![
            (5, b"key1", b"value5"),
            (4, b"key1", b"value4"),
            (3, b"key1", b""),
            (2, b"key1", b"value2"),
            (1, b"key2", b"value1"),
        ];

        for (seq, key, value) in cases.iter() {
            table.put(*seq, key, value);
        }

        let mut iter = table.iter();
        iter.rewind(false);
        assert!(iter.is_valid());
        for (i, k, v) in cases.iter() {
            let ik = get_user_key(iter.key().unwrap());
            let iv = iter.value().unwrap();
            assert_eq!(ik, *k);
            assert_eq!(iv, *v);
            iter.next();
        }

        iter.rewind(true);
        assert!(iter.is_valid());
        for (_, k, v) in cases.iter().rev() {
            let ik = get_user_key(iter.key().unwrap());
            let iv = iter.value().unwrap();
            assert_eq!(ik, *k);
            assert_eq!(iv, *v);
            iter.prev();
        }
    }
}
