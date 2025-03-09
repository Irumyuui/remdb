use std::{
    io::{self, Read, Seek, Write},
    path::Path,
};

use fs2::FileExt;

use crate::traits::{File, FileSystem};

pub type NativeFile = std::fs::File;

impl File for NativeFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        Read::read(self, buf)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        Read::read_to_end(self, buf)
    }

    #[cfg(unix)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::unix::fs::FileExt;
        FileExt::read_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        use std::os::windows::fs::FileExt;
        FileExt::seek_read(self, buf, offset)
    }

    #[cfg(not(any(unix, windows)))]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        unimplemented!()
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Write::write(self, buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Write::flush(self)
    }

    /// Nothing to do here, the file is closed when it goes out of scope
    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        Seek::seek(self, pos)
    }

    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|m| m.len())
    }

    fn lock(&self) -> io::Result<()> {
        self.try_lock_exclusive()
    }

    fn unlock(&self) -> io::Result<()> {
        std::fs::File::unlock(self)
    }
}

#[derive(Debug, Default, Clone)]
pub struct NativeFileSystem;

impl FileSystem for NativeFileSystem {
    type File = NativeFile;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
    }

    fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File> {
        std::fs::OpenOptions::new().read(true).open(path)
    }

    fn remove<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn mkdir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        std::fs::create_dir(path)
    }

    fn mkdir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn remove_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        std::fs::remove_dir(path)
    }

    fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
        path.as_ref().exists()
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;
    use scopeguard::defer;

    use crate::{native::NativeFileSystem, traits::FileSystem};

    #[test]
    fn test_api_native_file() -> Result<()> {
        let filepath = std::env::temp_dir().join("test_api_native_file.txt");
        defer! {
            let path = filepath.clone();
            let _ = std::fs::remove_file(path);
        }
        use std::io::Write;

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&filepath)?;
        file.write_all(b"If you want it, then you'll have to take it.")?;
        file.sync_all()?;

        let mut file = std::fs::File::open(&filepath)?;
        {
            use crate::traits::File;

            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            assert_eq!(buf, b"If you want it, then you'll have to take it.");
        }

        Ok(())
    }

    #[test]
    fn test_native_fs() -> Result<()> {
        let filepath = std::env::temp_dir().join("test_native_fs.txt");
        let fs = NativeFileSystem;

        defer! {
            let path = filepath.clone();
            let _ = std::fs::remove_file(path);
        };

        let mut file = fs.create(&filepath)?;
        {
            use crate::traits::File;
            file.write(b"If you want it, then you'll have to take it.")?;
            file.flush()?;
            file.close()?;
        }

        let mut file = fs.open(&filepath)?;
        {
            use crate::traits::File;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf)?;
            assert_eq!(buf, b"If you want it, then you'll have to take it.");
        }

        Ok(())
    }
}
