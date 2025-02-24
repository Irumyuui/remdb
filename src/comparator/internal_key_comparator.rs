use std::cmp::{self, Ordering::*};

use crate::format::{get_key_seq, get_user_key};

use super::Comparator;

pub struct InternalKeyComparator<C>
where
    C: Comparator,
{
    c: C,
}

impl<C> InternalKeyComparator<C>
where
    C: Comparator,
{
    pub fn new(c: C) -> Self {
        Self { c }
    }
}

impl<C> Default for InternalKeyComparator<C>
where
    C: Comparator + Default,
{
    fn default() -> Self {
        Self::new(C::default())
    }
}

impl<C> Clone for InternalKeyComparator<C>
where
    C: Comparator,
{
    fn clone(&self) -> Self {
        Self { c: self.c.clone() }
    }
}

impl<C> Comparator for InternalKeyComparator<C>
where
    C: Comparator,
{
    fn compare(&self, a: &[u8], b: &[u8]) -> cmp::Ordering {
        let a_key = get_user_key(a);
        let b_key = get_user_key(b);

        match self.c.compare(a_key, b_key) {
            Equal => {
                // seq cmp
                let a_seq = get_key_seq(a);
                let b_seq = get_key_seq(b);
                a_seq.cmp(&b_seq).reverse()
            }
            res => res,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering::*;

    use bytes::BufMut;

    use crate::comparator::prelude::*;

    fn make_key(key: &[u8], seq: u64) -> Vec<u8> {
        let mut res = key.to_vec();
        res.put_u64_le(seq);
        res
    }

    #[test]
    fn internal_key_compare() {
        let cmp = InternalKeyComparator::new(BytewiseComparator::default());

        assert_eq!(
            cmp.compare(
                make_key(b"key1", 1).as_slice(),
                make_key(b"key2", 3).as_slice()
            ),
            Less
        );
        assert_eq!(
            cmp.compare(
                make_key(b"key2", 1).as_slice(),
                make_key(b"key1", 0).as_slice()
            ),
            Greater
        );
        assert_eq!(
            cmp.compare(
                make_key(b"key", 1).as_slice(),
                make_key(b"key", 1).as_slice()
            ),
            Equal
        );
        assert_eq!(
            cmp.compare(
                make_key(b"key", 1).as_slice(),
                make_key(b"key", 2).as_slice()
            ),
            Greater
        );
        assert_eq!(
            cmp.compare(
                make_key(b"key", 2).as_slice(),
                make_key(b"key", 1).as_slice()
            ),
            Less
        );
    }
}
