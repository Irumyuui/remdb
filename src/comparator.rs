use std::cmp;

mod bytewise_comparator;
mod internal_key_comparator;

pub mod prelude {
    #![allow(unused)]

    pub use super::{
        Comparator, bytewise_comparator::BytewiseComparator,
        internal_key_comparator::InternalKeyComparator,
    };
}

pub trait Comparator: Send + Sync + Clone {
    fn compare(&self, a: &[u8], b: &[u8]) -> cmp::Ordering;
}
