use std::{
    collections::{BinaryHeap, binary_heap::PeekMut},
    future::Future,
};

use crate::{error::Result, key::KeySlice};

pub trait Iter: Send + Sync {
    type KeyType<'a>: PartialEq + Eq + PartialOrd + Ord
    where
        Self: 'a;

    fn key(&self) -> impl Future<Output = Self::KeyType<'_>> + Send;

    fn value(&self) -> impl Future<Output = &[u8]> + Send;

    fn is_valid(&self) -> impl Future<Output = bool> + Send;

    // TODO: use rewind?
    // fn rewind(&mut self) -> impl Future<Output = Result<()>> + Send;

    // TODO: seek
    // fn seek(&mut self, key: Self::KeyType<'_>) -> impl Future<Output = Result<()>> + Send;

    fn next(&mut self) -> impl Future<Output = Result<()>> + Send;
}

struct HeapWrapper<I>
where
    I: Iter,
{
    iter: Box<I>,
    idx: usize,
}

impl<I> PartialEq for HeapWrapper<I>
where
    I: Iter,
{
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<I> Eq for HeapWrapper<I> where I: Iter {}

impl<I> PartialOrd for HeapWrapper<I>
where
    I: Iter,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I> Ord for HeapWrapper<I>
where
    I: Iter,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        futures::executor::block_on(async {
            self.iter
                .key()
                .await
                .cmp(&other.iter.key().await)
                .then(self.idx.cmp(&other.idx))
                .reverse()
        })
    }
}

pub(crate) struct MergeIter<I>
where
    I: Iter,
{
    iter_heap: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I> MergeIter<I>
where
    I: Iter,
{
    pub async fn new(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid().await {
                heap.push(HeapWrapper { iter, idx });
            }
        }
        let current = heap.pop();

        Self {
            iter_heap: heap,
            current,
        }
    }
}

impl<I> Iter for MergeIter<I>
where
    I: for<'a> Iter<KeyType<'a> = KeySlice<'a>> + 'static,
{
    type KeyType<'a> = KeySlice<'a>;

    async fn key(&self) -> Self::KeyType<'_> {
        self.current.as_ref().unwrap().iter.key().await
    }

    async fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().iter.value().await
    }

    async fn is_valid(&self) -> bool {
        if let Some(ref w) = self.current
            && w.iter.is_valid().await
        {
            true
        } else {
            false
        }
    }

    async fn next(&mut self) -> Result<()> {
        let mut current = self.current.take().unwrap();
        let current_key = current.iter.key().await;
        while let Some(mut peek_iter) = self.iter_heap.peek_mut() {
            if peek_iter.iter.key().await == current_key {
                break;
            }

            if let Err(e) = peek_iter.iter.next().await {
                PeekMut::pop(peek_iter);
                return Err(e);
            }
            if !peek_iter.iter.is_valid().await {
                PeekMut::pop(peek_iter);
            }
        }

        current.iter.next().await?;
        if !current.iter.is_valid().await {
            self.current = self.iter_heap.pop();
            return Ok(());
        }

        let new_current = if let Some(next) = self.iter_heap.peek_mut()
            && current < *next
        {
            Some(PeekMut::pop(next))
        } else {
            None
        };
        if let Some(new_current) = new_current {
            self.iter_heap.push(current);
            self.current = Some(new_current);
        } else {
            self.current = Some(current);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::memtable::MemTable;

    #[tokio::test]
    async fn test_merge_iter() {
        todo!()
    }
}
