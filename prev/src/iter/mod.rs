use crate::error::Result;

pub trait Iter: Send + Sync {
    fn is_valid(&self) -> bool;

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];

    fn next(&mut self) -> Result<()>;

    fn is_rev(&self) -> bool {
        false
    }
}
