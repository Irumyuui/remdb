pub trait BasicHash {
    fn gen_basic_hash(&self) -> u32;
}

impl<T> BasicHash for T
where
    T: AsRef<[u8]>,
{
    fn gen_basic_hash(&self) -> u32 {
        basic_hash(self.as_ref(), 0xc6a4a793)
    }
}

pub fn basic_hash(data: &[u8], seed: u32) -> u32 {
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
