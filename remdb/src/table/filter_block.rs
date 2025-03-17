use bytes::{Buf, Bytes};

use super::bloom::Bloom;

#[derive(Clone)]
pub struct FilterBlock {
    data: Bytes,
}

impl FilterBlock {
    pub fn from_raw_data(data: Bytes) -> Self {
        Self { data }
    }

    pub fn into_raw_data(self) -> Bytes {
        self.data
    }

    pub fn is_valid(&self) -> bool {
        if self.data.len() < 8 {
            return false;
        }

        let n = self.data.len();
        let check_sum = self.data[n - 4..].as_ref().get_u32_le();
        let mut buf = self.data.slice(0..n - 4);
        let crc32 = crc32fast::hash(&buf);

        crc32 == check_sum
    }

    pub fn may_contains(&self, key: &[u8]) -> bool {
        Bloom::may_contain(&self.data, &key)
    }
}
