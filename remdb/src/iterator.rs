use std::future::Future;

use crate::error::Result;

pub trait Iterator: Send + Sync {
    type KeyType<'a>: PartialEq + Eq + PartialOrd + Ord
    where
        Self: 'a;

    fn key(&self) -> impl Future<Output = Self::KeyType<'_>> + Send;

    fn value(&self) -> impl Future<Output = Vec<u8>> + Send;

    fn is_valid(&self) -> impl Future<Output = bool> + Send;

    fn rewind(&mut self) -> impl Future<Output = Result<()>> + Send;

    fn next(&mut self) -> impl Future<Output = Result<()>> + Send;
}
