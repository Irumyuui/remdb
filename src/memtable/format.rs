use std::{ops::Bound, sync::Arc};

use bytes::Bytes;

use crate::{db::format::FileId, iter::Iter, options::Options};

pub trait ImplMemTable: Send + Sync + Clone {
    type IterType: Iter;

    fn create(id: FileId, options: Arc<Options>) -> Self;

    fn get(&self, key: &[u8]) -> Option<Bytes>;

    fn put(&self, key: Bytes, value: Bytes);

    fn approximate_size(&self) -> usize;

    // require seek on first key
    fn iter(&self, start: Bound<&[u8]>, limit: Bound<&[u8]>) -> Self::IterType;

    fn id(&self) -> &FileId;
}
