use std::cmp;

mod defalut_comparator;

pub mod prelude {
    #![allow(unused)]

    pub use super::Comparator;
    pub use super::defalut_comparator::DefaultComparator;
}

pub trait Comparator: Send + Sync + Clone {
    type Item;

    fn compare(&self, a: &Self::Item, b: &Self::Item) -> cmp::Ordering;
}
