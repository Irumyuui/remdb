use std::fmt::{Debug, Display};

#[derive(Default, Clone)]
pub struct BitMap {
    data: Vec<u64>,
}

const BIT_PER_ITEM: usize = std::mem::size_of::<u64>() * 8;

impl BitMap {
    pub fn new(bytes: usize) -> Self {
        let mut this = Self::default();
        this.resize(bytes);
        this
    }

    pub fn resize(&mut self, bytes: usize) {
        let need = bytes.div_ceil(BIT_PER_ITEM);
        self.data.resize(need, 0);
    }

    fn get_bit_pos(idx: usize) -> (usize, u64) {
        let pos = idx / BIT_PER_ITEM;
        let bit = idx % BIT_PER_ITEM;
        (pos, 1 << bit)
    }

    pub fn get(&self, idx: usize) -> bool {
        let (pos, mask) = Self::get_bit_pos(idx);
        self.data[pos] & mask != 0
    }

    pub fn set(&mut self, idx: usize) {
        let (pos, mask) = Self::get_bit_pos(idx);
        self.data[pos] |= mask;
    }

    pub fn len(&self) -> usize {
        self.data.len() * BIT_PER_ITEM
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl Debug for BitMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitMap").field("data", &self.data).finish()
    }
}

impl Display for BitMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for i in 0..self.len() {
            write!(f, "{}", if self.get(i) { "1" } else { "0" })?;
            if i > 0 {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
}
