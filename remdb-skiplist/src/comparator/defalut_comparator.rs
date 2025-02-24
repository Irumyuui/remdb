use std::{cmp, marker::PhantomData};

use super::Comparator;

#[derive(Debug, Default)]
pub struct DefaultComparator<T> {
    _marker: PhantomData<T>,
}

impl<T> Comparator for DefaultComparator<T>
where
    T: Send + Sync + Ord,
{
    type Item = T;

    fn compare(&self, a: &Self::Item, b: &Self::Item) -> cmp::Ordering {
        a.cmp(b)
    }
}

impl<T> Clone for DefaultComparator<T> {
    fn clone(&self) -> Self {
        Self {
            _marker: self._marker.clone(),
        }
    }
}

impl<T> Copy for DefaultComparator<T> {}
