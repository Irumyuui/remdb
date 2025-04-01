use std::{cmp::Reverse, collections::BinaryHeap};

use crate::error::Result;

use super::{KvItem, KvIter, Peekable};

struct HeapWrapper<I: KvIter> {
    current: KvItem,
    iter: I,
    index: usize,
}

impl<I: KvIter> HeapWrapper<I> {
    fn new(current: KvItem, iter: I, index: usize) -> Self {
        Self {
            current,
            iter,
            index,
        }
    }
}

impl<I: KvIter> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<I: KvIter> Eq for HeapWrapper<I> {}

impl<I: KvIter> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: KvIter> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.current
            .key
            .cmp(&other.current.key)
            .then(self.index.cmp(&other.index))
    }
}

pub struct MergeIter<I: KvIter> {
    current: Option<HeapWrapper<I>>,
    iters: BinaryHeap<Reverse<HeapWrapper<I>>>,
}

impl<I: KvIter> MergeIter<I> {
    pub async fn new(iters: Vec<I>) -> Result<Self> {
        let mut heap = BinaryHeap::new();
        for (index, mut iter) in iters.into_iter().enumerate() {
            if let Some(item) = iter.next().await? {
                heap.push(Reverse(HeapWrapper::new(item, iter, index)));
            }
        }

        Ok(Self {
            current: heap.pop().map(|Reverse(i)| i),
            iters: heap,
        })
    }

    async fn advance(&mut self) -> Result<Option<KvItem>> {
        let res = match self.current.take() {
            Some(mut current_iter) => {
                let current_item = current_iter.current;
                if let Some(next_item) = current_iter.iter.next().await? {
                    current_iter.current = next_item;
                    self.iters.push(Reverse(current_iter));
                }
                self.current = self.iters.pop().map(|Reverse(i)| i);
                Some(current_item)
            }
            None => None,
        };
        Ok(res)
    }
}

impl<I: KvIter> KvIter for MergeIter<I> {
    async fn next(&mut self) -> Result<Option<KvItem>> {
        let Some(mut current_item) = self.advance().await? else {
            return Ok(None);
        };

        while let Some(peek) = self.peek() {
            if peek.key != current_item.key {
                break;
            }
            self.advance().await?;
        }

        Ok(Some(current_item))
    }
}

impl<I: KvIter> Peekable for MergeIter<I> {
    fn peek(&self) -> Option<&KvItem> {
        self.current.as_ref().map(|w| &w.current)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;

    use crate::{
        format::key::KeyBytes,
        kv_iter::{KvIter, tests::MockData},
        test_utils::init_tracing_not_failed,
    };

    #[tokio::test]
    async fn test_merge_iter() {
        init_tracing_not_failed();

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
        let iters = mock_datas.iter().map(|data| data.iter()).collect_vec();
        let mut merge_iter = super::MergeIter::new(iters)
            .await
            .expect("should build merge iter");

        let mut actual = vec![];
        while let Some(item) = merge_iter
            .next()
            .await
            .expect("merge iter should have next")
        {
            actual.push((item.key.clone(), item.value.value_or_ptr.clone()));
        }

        let mut expected = vec![];
        for data in mock_datas.iter() {
            for (key, value) in data.items.iter() {
                expected.push((key.clone(), value.clone()));
            }
        }
        expected.sort();

        assert_eq!(expected.len(), actual.len());
        for (exp, atc) in expected.iter().zip(actual.iter()) {
            assert_eq!(exp, atc);
        }
    }
}
