use std::path::PathBuf;

#[derive(Clone)]
pub enum FileType {
    MemTable,
}

#[derive(Clone)]
pub struct FileId {
    pub(crate) ty: FileType,
    pub(crate) id: u64,
    pub(crate) path: PathBuf,
}
