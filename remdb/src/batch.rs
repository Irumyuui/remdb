use std::fmt::Debug;

use crate::core::WrireRecord;

pub struct WriteBatch {
    pub(crate) batch: Vec<WrireRecord<Vec<u8>>>,
}

impl Debug for WriteBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteBatch")
            .field("batch", &self.batch)
            .finish()
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self { batch: Vec::new() }
    }
}

impl WriteBatch {
    pub fn put<T: AsRef<[u8]>>(&mut self, key: T, value: T) {
        self.batch.push(WrireRecord::Put(
            key.as_ref().to_vec(),
            value.as_ref().to_vec(),
        ));
    }

    pub fn delete<T: AsRef<[u8]>>(&mut self, key: T) {
        self.batch.push(WrireRecord::Delete(key.as_ref().to_vec()));
    }

    #[allow(unused)]
    pub fn into_batch(self) -> Vec<WrireRecord<Vec<u8>>> {
        self.batch
    }
}
