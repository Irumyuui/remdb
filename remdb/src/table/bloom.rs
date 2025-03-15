#![allow(unused)]

use std::f64::consts::LN_2;

pub trait BloomHash {
    fn bloom_hash(&self) -> u32;
}

impl<T> BloomHash for T
where
    T: AsRef<[u8]>,
{
    fn bloom_hash(&self) -> u32 {
        basic_hash(self.as_ref(), 0xc6a4a793)
    }
}

fn basic_hash(data: &[u8], seed: u32) -> u32 {
    let m: u32 = 0xc6a4a793;
    let mut h = seed ^ (m.wrapping_mul(data.len() as u32));

    let mut i = 0;
    while i + 4 <= data.len() {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(&data[i..i + 4]);
        let w = u32::from_le_bytes(bytes);
        h = h.wrapping_add(w).wrapping_mul(m);
        h ^= h >> 16;
        i += 4;
    }

    let less = data.len() - i;
    if less >= 3 {
        h = h.wrapping_add((data[i + 2] as u32) << 16);
    }
    if less >= 2 {
        h = h.wrapping_add((data[i + 1] as u32) << 8);
    }
    if less >= 1 {
        h = h.wrapping_add(data[i] as u32).wrapping_mul(m);
        h ^= h >> 24;
    }
    h
}

#[derive(Debug, Clone, Copy)]
pub struct Bloom {
    k_num: u8,
    bits_per_key: usize,
}

impl Bloom {
    pub fn new(bits_per_key: usize) -> Self {
        // ln2 * (m / n)
        Self {
            k_num: (((bits_per_key as f64 * 0.69) as usize).max(1).min(30) as u8),
            bits_per_key,
        }
    }

    pub fn with_size_and_false_rate(len: usize, false_rate: f64) -> Self {
        let size = -1.0 * (len as f64) * false_rate.ln() / LN_2.powi(2);
        let bits_per_key = (size / (len as f64)).ceil() as usize;
        Self::new(bits_per_key)
    }

    pub fn build<T>(&self, keys: &[T]) -> Vec<u8>
    where
        T: BloomHash,
    {
        let bits = (keys.len() * self.bits_per_key).max(64);
        let bytes = (bits + 7) / 8;
        let bits = bytes * 8;

        let mut filter = vec![0u8; bytes + 1];
        filter[bytes] = self.k_num;

        for key in keys {
            let mut h = Self::hash(key);
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k_num {
                let bit_pos = h % bits as u32;
                filter[bit_pos as usize / 8] |= 1 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }

        filter
    }

    pub fn build_from_hashs(&self, key_hashs: &[u32]) -> Vec<u8> {
        let bits = (key_hashs.len() * self.bits_per_key).max(64);
        let bytes = (bits + 7) / 8;
        let bits = bytes * 8;

        let mut filter = vec![0u8; bytes + 1];
        filter[bytes] = self.k_num;

        for h in key_hashs {
            let mut h = *h;
            let delta = (h >> 17) | (h << 15);
            for _ in 0..self.k_num {
                let bit_pos = h % bits as u32;
                filter[bit_pos as usize / 8] |= 1 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }

        filter
    }

    pub fn may_contain<T>(filter: &[u8], key: &T) -> bool
    where
        T: BloomHash,
    {
        if filter.len() < 1 {
            return false;
        }

        let k = filter.last().unwrap().clone();
        if k > 30 {
            return true;
        }

        let bits = (filter.len() - 1) * 8;
        let mut h = Self::hash(key);
        let delta = (h >> 17) | (h << 15);
        for _ in 0..k {
            let bit_pos = h % (bits as u32);
            if (filter[bit_pos as usize / 8] & (1 << (bit_pos % 8))) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    pub fn hash<T>(data: &T) -> u32
    where
        T: BloomHash,
    {
        data.bloom_hash()
    }
}

#[cfg(test)]
mod tests {
    use crate::table::bloom::Bloom;

    #[test]
    fn empty_should_not_found() {
        let builder = Bloom::new(10);
        let empty_ref: &[&[u8]] = &[];
        let filter = builder.build(empty_ref);

        assert!(!Bloom::may_contain(&filter, &"key1".as_bytes()));
        assert!(!Bloom::may_contain(&filter, &"key2".as_bytes()));
        assert!(!Bloom::may_contain(&filter, &"empty".as_bytes()));
    }

    #[test]
    fn must_contain() {
        let builder = Bloom::new(10);
        let keys = vec!["key1".as_bytes(), "key2".as_bytes()];
        let filter = builder.build(&keys);

        assert!(Bloom::may_contain(&filter, &"key1".as_bytes()));
        assert!(Bloom::may_contain(&filter, &"key2".as_bytes()));
    }

    #[test]
    fn may_contains() {
        let builder = Bloom::new(10);

        let mut mediocre = 0;
        let mut good = 0;
        for len in [1, 10, 100, 1000, 10000] {
            let keys: Vec<Vec<u8>> = (0..len)
                .map(|i| i.to_string().as_bytes().to_vec())
                .collect();
            let filter = builder.build(&keys);

            // must be contains
            for key in keys.iter() {
                assert!(Bloom::may_contain(&filter, key), "key: {:?}", key);
            }

            // false check
            let mut hits = 0;
            for i in 0..10000 {
                let key = (i + 1000000000).to_string().as_bytes().to_vec();
                if Bloom::may_contain(&filter, &key) {
                    hits += 1;
                }
            }
            let rate = f64::from(hits) / 10000.;
            assert!(rate < 0.02, "rate: {}, len: {}", rate, len);

            if rate > 0.125 {
                mediocre += 1;
            } else {
                good += 1;
            }
        }

        assert!(
            mediocre * 5 < good,
            "mediocre: {}, good: {}",
            mediocre,
            good
        );
    }
}
