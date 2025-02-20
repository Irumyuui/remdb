#![allow(unused)] // TODO: Remove this line after implementation

// TODO: Add tempfile implement? Or use fs in memory?

use std::path::Path;

pub mod cross_fs_ext;
pub mod raw_fs;

pub trait FileObject: Send + Sync {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize>;

    fn read_at(&mut self, buf: &mut [u8], offset: u64) -> std::io::Result<usize>;

    fn read_exact_at(&mut self, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    buf = &mut buf[n..];
                    offset += n as u64;
                }
                Err(e) => {
                    if matches!(e.kind(), std::io::ErrorKind::Interrupted) {
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        if buf.is_empty() {
            return Ok(());
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "failed to fill whole buffer",
        ))
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize>;

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize>;

    fn flush(&mut self) -> std::io::Result<()>;

    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64>;

    fn lock(&self) -> std::io::Result<()>;

    fn unlock(&self) -> std::io::Result<()>;

    fn sync(&self) -> std::io::Result<()>;
}

pub trait FileSystem: Send + Sync {
    type File: FileObject;

    fn open<P: AsRef<Path>>(&self, options: OpenOptions, path: P) -> std::io::Result<Self::File>;

    fn remove<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()>;

    fn remove_dir<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()>;

    fn exists<P: AsRef<Path>>(&self, path: P) -> bool;

    fn sync(&self, file: &Self::File) -> std::io::Result<()> {
        file.sync()
    }

    fn sync_dir<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()>;
}

#[derive(Debug, Clone)]
pub struct OpenOptions {
    read: bool,
    write: bool,
    create: bool,
    truncate: bool,
    append: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            read: false,
            write: false,
            create: false,
            truncate: false,
            append: false,
        }
    }
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn read(&mut self) -> &mut Self {
        self.read = true;
        self
    }

    pub fn write(&mut self) -> &mut Self {
        self.write = true;
        self
    }

    pub fn create(&mut self) -> &mut Self {
        self.create = true;
        self
    }

    pub fn truncate(&mut self) -> &mut Self {
        self.truncate = true;
        self
    }

    pub fn append(&mut self) -> &mut Self {
        self.append = true;
        self
    }

    pub fn open<FS: FileSystem, P: AsRef<Path>>(
        self,
        fs: &FS,
        path: P,
    ) -> std::io::Result<FS::File> {
        fs.open(self, path)
    }
}
