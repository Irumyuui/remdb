use crate::error::Result;

pub trait Iter {
    fn prev(&mut self);

    fn next(&mut self);

    fn key(&self) -> Option<&[u8]>;

    fn value(&self) -> Option<&[u8]>;

    fn rewind(&mut self, from_last: bool);

    fn seek(&mut self, key: &[u8]);

    fn is_valid(&self) -> bool;

    fn status(&mut self) -> Result<()>;
}
