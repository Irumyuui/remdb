#![allow(unused)]

use std::{
    collections::{BinaryHeap, binary_heap::PeekMut},
    future::Future,
};

use crate::{
    error::Result,
    format::{key::KeySlice, value::Value},
};

pub trait Iter: Send + Sync {
    type KeyType<'a>: PartialEq + Eq + PartialOrd + Ord + Send + Sync
    where
        Self: 'a;

    fn key(&self) -> impl Future<Output = Self::KeyType<'_>> + Send;

    fn value(&self) -> impl Future<Output = Value> + Send;

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

    async fn value(&self) -> Value {
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
            if peek_iter.iter.key().await != current_key {
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

// A first
pub struct TwoMergeIter<A, B>
where
    A: Iter,
    B: Iter,
{
    a: A,
    b: B,
    use_a_flag: bool,
}

impl<A, B> TwoMergeIter<A, B>
where
    A: Iter + 'static,
    B: Iter + 'static + for<'a> Iter<KeyType<'a> = A::KeyType<'a>>,
{
    async fn update_use_flag(&mut self) {
        self.use_a_flag = if !self.a.is_valid().await {
            false
        } else if !self.b.is_valid().await {
            true
        } else {
            self.a.key().await < self.b.key().await
        };

        eprintln!("use_a res: {}", self.use_a_flag);
    }

    async fn skip_b(&mut self) -> Result<()> {
        while self.a.is_valid().await
            && self.b.is_valid().await
            && self.a.key().await == self.b.key().await
        {
            eprintln!("skip b!");
            self.b.next().await?;
        }
        Ok(())
    }

    pub async fn new(a: A, b: B) -> Result<Self> {
        let mut this = Self {
            a,
            b,
            use_a_flag: true,
        };
        this.skip_b().await?;
        this.update_use_flag().await;
        Ok(this)
    }
}

macro_rules! with_current_iter {
    ($self:ident, $method:ident($($args:expr),*)) => {
        if $self.use_a_flag {
            eprintln!("use a");
            $self.a.$method($($args),*).await
        } else {
            eprintln!("use b");
            $self.b.$method($($args),*).await
        }
    };
}

impl<A, B> Iter for TwoMergeIter<A, B>
where
    A: Iter + 'static,
    B: Iter + 'static + for<'a> Iter<KeyType<'a> = A::KeyType<'a>>,
{
    type KeyType<'a> = A::KeyType<'a>;

    async fn is_valid(&self) -> bool {
        with_current_iter!(self, is_valid())
    }

    async fn key(&self) -> Self::KeyType<'_> {
        with_current_iter!(self, key())
    }

    async fn value(&self) -> Value {
        with_current_iter!(self, value())
    }

    async fn next(&mut self) -> Result<()> {
        with_current_iter!(self, next())?;

        self.skip_b().await?;
        self.update_use_flag().await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fmt::Debug, sync::Arc};

    use bytes::Bytes;
    use itertools::Itertools;

    use crate::{
        error::Result,
        format::{
            key::{KeyBytes, KeySlice},
            value::Value,
        },
    };

    use super::Iter;

    #[derive(Clone)]
    struct MockData {
        items: Arc<Vec<(KeyBytes, Bytes)>>,
    }

    impl MockData {
        fn new(mut items: Vec<(KeyBytes, Bytes)>) -> Self {
            items.sort_by(|a, b| a.0.cmp(&b.0));
            Self {
                items: Arc::new(items),
            }
        }

        fn iter(&self) -> MockIter {
            MockIter::new(self.clone())
        }
    }

    impl Debug for MockData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockData")
                .field("items", &self.items)
                .finish()
        }
    }

    struct MockIter {
        data: MockData,
        idx: usize,
    }

    impl Debug for MockIter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockIter")
                .field("data", &self.data)
                .field("idx", &self.idx)
                .finish()
        }
    }

    impl MockIter {
        fn new(data: MockData) -> MockIter {
            Self { data, idx: 0 }
        }
    }

    impl Iter for MockIter {
        type KeyType<'a> = KeySlice<'a>;

        async fn key(&self) -> Self::KeyType<'_> {
            self.data.items[self.idx].0.as_key_slice()
        }

        async fn value(&self) -> Value {
            Value::from_raw_slice(self.data.items[self.idx].1.as_ref())
        }

        async fn is_valid(&self) -> bool {
            self.idx < self.data.items.len()
        }

        async fn next(&mut self) -> Result<()> {
            self.idx += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_merge_iter() {
        let mut mock_datas = vec![vec![]; 3];

        let mut last_ts = 0;
        for cnt in 0..10 {
            for data in mock_datas.iter_mut() {
                let key = Bytes::from(format!("key-{:05}", cnt));
                let ts = last_ts;
                let key = KeyBytes::new(key, ts);
                let value = Bytes::from(format!("value-{:05}", cnt * 10));
                data.push((key, value));
                last_ts += 1;
            }
            mock_datas.reverse();
        }

        let mock_datas = mock_datas.into_iter().map(MockData::new).collect_vec();
        let iters = mock_datas
            .iter()
            .map(|data| Box::new(data.iter()) as Box<MockIter>)
            .collect_vec();
        let mut merge_iter = super::MergeIter::new(iters).await;

        let mut actual = vec![];
        while merge_iter.is_valid().await {
            actual.push((
                merge_iter.key().await.into_key_bytes(),
                Bytes::copy_from_slice(&merge_iter.value().await.value_or_ptr),
            ));
            merge_iter.next().await.unwrap();
        }

        let mut expected = vec![];
        for data in mock_datas.iter() {
            for (key, value) in data.items.iter() {
                expected.push((key.clone(), value.clone()));
            }
        }
        expected.sort();

        for (exp, atc) in expected.iter().zip(actual.iter()) {
            assert_eq!(exp, atc);
        }
    }

    #[tokio::test]
    async fn test_two_merge_iter() {
        let mut data_a = vec![];
        for i in 0..5 {
            let key = Bytes::from(format!("key-{:05}", i));
            let ts = i as u64;
            let key = KeyBytes::new(key, ts);
            let value = Bytes::from(format!("value-a-{:05}", i));
            data_a.push((key, value));
        }

        let mut data_b = vec![];
        for i in 2..7 {
            let key = Bytes::from(format!("key-{:05}", i));
            let ts = i as u64;
            let key = KeyBytes::new(key, ts);
            let value = Bytes::from(format!("value-b-{:05}", i));
            data_b.push((key, value));
        }

        let mock_data_a = MockData::new(data_a);
        let mock_data_b = MockData::new(data_b);
        let iter_a = mock_data_a.iter();
        let iter_b = mock_data_b.iter();

        let mut merge_iter = super::TwoMergeIter::new(iter_a, iter_b).await.unwrap();

        let mut actual = vec![];
        while merge_iter.is_valid().await {
            actual.push((
                merge_iter.key().await.into_key_bytes(),
                Bytes::copy_from_slice(&merge_iter.value().await.value_or_ptr),
            ));
            merge_iter.next().await.unwrap();
        }

        let mut expected = vec![];
        let mut keys_from_a = mock_data_a
            .items
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();

        for (key, value) in mock_data_a.items.iter() {
            expected.push((key.clone(), value.clone()));
        }

        for (key, value) in mock_data_b.items.iter() {
            if !keys_from_a.contains(key) {
                expected.push((key.clone(), value.clone()));
            }
        }

        expected.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(actual.len(), expected.len());
        for (exp, act) in expected.iter().zip(actual.iter()) {
            assert_eq!(exp, act);
        }
    }

    #[tokio::test]
    async fn test_two_merge_iter_a_is_empty() -> anyhow::Result<()> {
        let mock_data_a = MockData::new(vec![]);
        let mut data_b = vec![];
        for i in 0..3 {
            let key = Bytes::from(format!("key-{:05}", i));
            let ts = i as u64;
            let key = KeyBytes::new(key, ts);
            let value = Bytes::from(format!("value-b-{:05}", i));
            data_b.push((key, value));
        }
        let mock_data_b = MockData::new(data_b);

        let iter_a = mock_data_a.iter();
        let iter_b = mock_data_b.iter();

        let mut merge_iter = super::TwoMergeIter::new(iter_a, iter_b).await?;

        let mut actual = vec![];
        while merge_iter.is_valid().await {
            actual.push((
                merge_iter.key().await.into_key_bytes(),
                Bytes::copy_from_slice(&merge_iter.value().await.value_or_ptr),
            ));
            merge_iter.next().await?;
        }

        assert_eq!(actual.len(), mock_data_b.items.len());
        for ((key_a, val_a), (key_b, val_b)) in actual.iter().zip(mock_data_b.items.iter()) {
            assert_eq!(key_a, key_b);
            assert_eq!(val_a, val_b);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_two_merge_iter_b_is_empty() -> anyhow::Result<()> {
        let mut data_a = vec![];
        for i in 0..3 {
            let key = Bytes::from(format!("key-{:05}", i));
            let ts = i as u64;
            let key = KeyBytes::new(key, ts);
            let value = Bytes::from(format!("value-a-{:05}", i));
            data_a.push((key, value));
        }
        let mock_data_a = MockData::new(data_a);
        let mock_data_b = MockData::new(vec![]);

        let iter_a = mock_data_a.iter();
        let iter_b = mock_data_b.iter();

        let mut merge_iter = super::TwoMergeIter::new(iter_a, iter_b).await?;

        let mut actual = vec![];
        while merge_iter.is_valid().await {
            actual.push((
                merge_iter.key().await.into_key_bytes(),
                Bytes::copy_from_slice(&merge_iter.value().await.value_or_ptr),
            ));
            merge_iter.next().await?;
        }

        assert_eq!(actual.len(), mock_data_a.items.len());
        for ((key_a, val_a), (key_b, val_b)) in actual.iter().zip(mock_data_a.items.iter()) {
            assert_eq!(key_a, key_b);
            assert_eq!(val_a, val_b);
        }

        Ok(())
    }
}
