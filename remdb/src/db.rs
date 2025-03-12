use std::sync::Arc;

use crate::memtable::MemTable;

pub struct Core {
    mem: Arc<MemTable>,
    imms: Vec<Arc<MemTable>>,
}
