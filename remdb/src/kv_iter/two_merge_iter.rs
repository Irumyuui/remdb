use crate::error::Result;

use super::{KvItem, KvIter, Peekable};

struct Entry<I: KvIter> {
    iter: I,
    current: Option<KvItem>,
}

impl<I: KvIter> Entry<I> {
    async fn new(mut iter: I) -> Result<Self> {
        let current = iter.next().await?;
        Ok(Self { iter, current })
    }

    async fn advance(&mut self) -> Result<Option<KvItem>> {
        if self.current.is_none() {
            return Ok(None);
        }

        let current = self.current.take();
        self.current = self.iter.next().await?;
        Ok(current)
    }

    fn peek(&self) -> Option<&KvItem> {
        self.current.as_ref()
    }
}

pub struct TwoMergeIter<A: KvIter, B: KvIter> {
    a: Entry<A>,
    b: Entry<B>,
}

impl<A: KvIter, B: KvIter> TwoMergeIter<A, B> {
    pub async fn new(a: A, b: B) -> Result<Self> {
        Ok(Self {
            a: Entry::new(a).await?,
            b: Entry::new(b).await?,
        })
    }

    async fn advance(&mut self) -> Result<Option<KvItem>> {
        match (self.a.peek(), self.b.peek()) {
            (None, None) => Ok(None),
            (None, Some(_)) => self.b.advance().await,
            (Some(_), None) => self.a.advance().await,
            (Some(item1), Some(item2)) => {
                if item1.key < item2.key {
                    self.a.advance().await
                } else {
                    self.b.advance().await
                }
            }
        }
    }
}

impl<A: KvIter, B: KvIter> KvIter for TwoMergeIter<A, B> {
    async fn next(&mut self) -> Result<Option<KvItem>> {
        let current_item = self.advance().await?;

        while let (Some(a), Some(b)) = (self.a.peek(), self.b.peek()) {
            if a.key.key() == b.key.key() {
                self.b.advance().await?;
            } else {
                break;
            }
        }

        Ok(current_item)
    }
}

impl<A: KvIter, B: KvIter> Peekable for TwoMergeIter<A, B> {
    fn peek(&self) -> Option<&KvItem> {
        match (self.a.peek(), self.b.peek()) {
            (None, None) => todo!(),
            (None, Some(b)) => Some(b),
            (Some(a), None) => Some(a),
            (Some(a), Some(b)) => {
                if a.key < b.key {
                    Some(a)
                } else {
                    Some(b)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{
        format::key::KeyBytes,
        kv_iter::{KvIter, tests::MockData},
        test_utils::init_tracing_not_failed,
    };

    use super::TwoMergeIter;

    #[tokio::test]
    async fn test_to_merge_iter() {
        init_tracing_not_failed();

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

        let mut merge_iter = TwoMergeIter::new(iter_a, iter_b).await.unwrap();

        let mut actual = vec![];
        while let Some(item) = merge_iter
            .next()
            .await
            .expect("merge iter should have next")
        {
            actual.push((item.key.clone(), item.value.value_or_ptr.clone()));
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
}
