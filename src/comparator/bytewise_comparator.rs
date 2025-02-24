use super::Comparator;

#[derive(Clone, Copy, Debug, Default)]
pub struct BytewiseComparator;

impl Comparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use crate::comparator::Comparator;

    #[test]
    fn bytewise_compare() {
        let cmp = super::BytewiseComparator;
        assert_eq!(cmp.compare(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(cmp.compare(b"abc", b"def"), Ordering::Less);
        assert_eq!(cmp.compare(b"def", b"abc"), Ordering::Greater);
    }
}
