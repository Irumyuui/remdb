use std::{
    io::{self, SeekFrom},
    path::Path,
};

pub mod prelude {
    pub use super::{File, FileSystem};
}

/// Read + Write + Seek
pub trait File: Send + Sync {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize>;

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize>;

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
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
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ))
        }
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize>;

    fn flush(&mut self) -> io::Result<()>;

    fn close(&mut self) -> io::Result<()>;

    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64>;

    fn len(&self) -> io::Result<u64>;

    fn is_empty(&self) -> bool {
        self.len().map(|len| len == 0).unwrap_or(false)
    }

    fn lock(&self) -> io::Result<()>;

    fn unlock(&self) -> io::Result<()>;
}

pub trait FileSystem: Send + Sync {
    type File: File;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File>;

    fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File>;

    fn remove<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;

    fn mkdir<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;

    fn mkdir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;

    fn remove_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;

    // ?
    // fn sync<'a, I>(&self, files: I) -> io::Result<()>
    // where
    //     I: IntoIterator<Item = &'a Self::File>,
    //     <Self as Storage>::File: 'a;

    fn exists<P: AsRef<Path>>(&self, path: P) -> bool;
}
