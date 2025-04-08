#![allow(unused)]

use crate::{
    error::KvResult,
    format::{key::KeyBytes, value::Value},
};

mod db_iter;
mod merge_iter;
mod two_merge_iter;

pub mod prelude {
    pub use super::{KvItem, KvIter};
    pub(crate) use super::{
        Peekable,
        db_iter::{DbMergeIter, DbMergeIterInner},
        merge_iter::MergeIter,
        two_merge_iter::TwoMergeIter,
    };
}

#[derive(Debug, Clone)]
pub struct KvItem {
    pub(crate) key: KeyBytes,
    pub(crate) value: Value,
}

impl KvItem {
    #[cfg(test)]
    pub fn from_fake_item(key: KeyBytes, value: bytes::Bytes) -> Self {
        use bytes::Bytes;

        Self {
            key,
            value: Value::from_raw_value(value),
        }
    }
}

impl PartialEq for KvItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl PartialOrd for KvItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for KvItem {}

impl Ord for KvItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

pub trait KvIter: Send + Sync {
    fn next(&mut self) -> impl Future<Output = KvResult<Option<KvItem>>> + Send;
}

/// Why need split `peek`? Because it doesn't need to be `async`, just read from memory.
pub(crate) trait Peekable: KvIter {
    fn peek(&self) -> Option<&KvItem>;
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{fmt::Debug, sync::Arc};

    use bytes::Bytes;

    use crate::{
        format::{key::KeyBytes, value::Value},
        prelude::KvResult,
    };

    use super::{KvItem, KvIter, Peekable};

    #[derive(Debug, Clone)]
    pub struct MockData {
        pub items: Arc<Vec<(KeyBytes, Bytes)>>,
    }

    impl MockData {
        pub fn new(mut items: Vec<(KeyBytes, Bytes)>) -> Self {
            items.sort_by(|a, b| a.0.cmp(&b.0));
            Self {
                items: Arc::new(items),
            }
        }

        pub fn iter(&self) -> MockIter {
            MockIter::new(self.clone())
        }
    }

    #[derive(Debug)]
    pub struct MockIter {
        data: MockData,
        idx: usize,
    }

    impl MockIter {
        fn new(data: MockData) -> MockIter {
            Self { data, idx: 0 }
        }

        pub(crate) fn peek(&self) -> Option<KvItem> {
            if self.idx < self.data.items.len() {
                let (key, value) = self.data.items[self.idx].clone();
                Some(KvItem {
                    key,
                    value: Value::from_raw_value(value),
                })
            } else {
                None
            }
        }
    }

    impl KvIter for MockIter {
        async fn next(&mut self) -> KvResult<Option<KvItem>> {
            let res = match self.data.items.get(self.idx) {
                Some(_) => {
                    let (key, value) = self.data.items[self.idx].clone();
                    self.idx += 1;
                    Some(KvItem {
                        key,
                        value: Value::from_raw_value(value),
                    })
                }
                None => None,
            };
            Ok(res)
        }
    }
}
