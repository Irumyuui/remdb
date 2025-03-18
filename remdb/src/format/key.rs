#![allow(unused)]

use std::{cmp::Reverse, fmt::Debug, mem};

use bytes::Bytes;

pub type Seq = u64;

pub struct Key<T>
where
    T: AsRef<[u8]>,
{
    pub(crate) real_key: T,
    seq: Seq,
}

impl<T> Debug for Key<T>
where
    T: AsRef<[u8]>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(key) = String::from_utf8(self.real_key.as_ref().to_vec()) {
            f.debug_struct("Key")
                .field("real_key", &key)
                .field("seq", &self.seq)
                .finish()
        } else {
            f.debug_struct("Key")
                .field("real_key", &self.real_key.as_ref())
                .field("seq", &self.seq)
                .finish()
        }
    }
}

impl<T> Default for Key<T>
where
    T: AsRef<[u8]> + Default,
{
    fn default() -> Self {
        Self {
            real_key: Default::default(),
            seq: Default::default(),
        }
    }
}

impl<T> PartialEq for Key<T>
where
    T: AsRef<[u8]> + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.real_key.as_ref() == other.real_key.as_ref() && self.seq == other.seq
    }
}

impl<T> Eq for Key<T> where T: AsRef<[u8]> + Eq {}

impl<T> Clone for Key<T>
where
    T: AsRef<[u8]> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            real_key: self.real_key.clone(),
            seq: self.seq,
        }
    }
}

impl<T> PartialOrd for Key<T>
where
    T: AsRef<[u8]> + PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.real_key.as_ref(), Reverse(self.seq))
            .partial_cmp(&(other.real_key.as_ref(), Reverse(other.seq)))
    }
}

impl<T> Ord for Key<T>
where
    T: AsRef<[u8]> + Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.real_key.as_ref(), Reverse(self.seq))
            .cmp(&(other.real_key.as_ref(), Reverse(other.seq)))
    }
}

pub type KeySlice<'a> = Key<&'a [u8]>;
pub type KeyBuf = Key<Vec<u8>>;
pub type KeyBytes = Key<Bytes>;

impl Copy for KeySlice<'_> {}

impl<T> Key<T>
where
    T: AsRef<[u8]>,
{
    pub fn into_real_key(self) -> T {
        self.real_key
    }

    pub fn key_len(&self) -> usize {
        self.real_key.as_ref().len()
    }

    pub fn raw_len(&self) -> usize {
        self.real_key.as_ref().len() + mem::size_of::<Seq>()
    }

    pub fn is_empty(&self) -> bool {
        self.real_key.as_ref().is_empty()
    }

    pub fn key(&self) -> &[u8] {
        self.real_key.as_ref()
    }

    pub fn seq(&self) -> Seq {
        self.seq
    }
}

impl<'a> KeySlice<'a> {
    pub fn into_key_buf(self) -> KeyBuf {
        KeyBuf {
            real_key: self.real_key.to_vec(),
            seq: self.seq,
        }
    }

    pub fn into_key_bytes(self) -> KeyBytes {
        KeyBytes {
            real_key: Bytes::copy_from_slice(self.real_key),
            seq: self.seq,
        }
    }

    pub fn new(key: &'a [u8], seq: Seq) -> Self {
        Self { real_key: key, seq }
    }
}

impl KeyBuf {
    pub fn new(real_key: Vec<u8>, seq: Seq) -> Self {
        Self { real_key, seq }
    }

    pub fn clear(&mut self) {
        self.real_key.clear();
    }

    pub fn append(&mut self, other: impl AsRef<[u8]>) {
        self.real_key.extend_from_slice(other.as_ref());
    }

    pub fn set_seq(&mut self, seq: Seq) {
        self.seq = seq;
    }

    pub fn reset_from_slice(&mut self, slice: KeySlice) {
        self.real_key.clear();
        self.real_key.extend_from_slice(slice.real_key.as_ref());
        self.seq = slice.seq;
    }

    pub fn as_key_slice(&self) -> KeySlice {
        KeySlice {
            real_key: self.real_key.as_slice(),
            seq: self.seq,
        }
    }

    pub fn into_key_bytes(self) -> KeyBytes {
        KeyBytes {
            real_key: Bytes::from(self.real_key),
            seq: self.seq,
        }
    }
}

impl KeyBytes {
    pub fn new(real_key: Bytes, seq: Seq) -> Self {
        Self { real_key, seq }
    }

    pub fn as_key_slice(&self) -> KeySlice {
        KeySlice {
            real_key: self.real_key.as_ref(),
            seq: self.seq,
        }
    }
}
