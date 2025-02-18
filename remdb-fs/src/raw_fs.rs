use std::{
    fs::File,
    io::{BufReader, Read, Seek, Write},
};

use fs2::FileExt;

use crate::{FileObject, FileSystem, cross_fs_ext::CrossFileExt};

impl FileObject for File {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Read::read(self, buf)
    }

    fn read_at(&mut self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        CrossFileExt::read_at(self, buf, offset)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> std::io::Result<usize> {
        BufReader::new(self).read_to_end(buf)
    }

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Write::write(self, buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Write::flush(self)
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        Seek::seek(self, pos)
    }

    fn lock(&self) -> std::io::Result<()> {
        FileExt::try_lock_exclusive(self)
    }

    fn unlock(&self) -> std::io::Result<()> {
        FileExt::unlock(self)
    }

    fn sync(&self) -> std::io::Result<()> {
        self.sync_all()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RawFileSystem;

impl FileSystem for RawFileSystem {
    type File = File;

    fn open<P: AsRef<std::path::Path>>(
        &self,
        options: crate::OpenOptions,
        path: P,
    ) -> std::io::Result<Self::File> {
        let file = std::fs::OpenOptions::new()
            .read(options.read)
            .write(options.write)
            .create(options.create)
            .truncate(options.truncate)
            .append(options.append)
            .open(path)?;
        Ok(file)
    }

    fn remove<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        std::fs::remove_file(path)
    }

    fn remove_dir<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        std::fs::remove_dir(path)
    }

    fn exists<P: AsRef<std::path::Path>>(&self, path: P) -> bool {
        path.as_ref().exists()
    }

    fn sync_dir<P: AsRef<std::path::Path>>(&self, path: P) -> std::io::Result<()> {
        std::fs::File::open(path)?.sync_all()
    }
}
