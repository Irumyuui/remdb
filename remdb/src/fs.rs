#![allow(unused)]

use std::{io, path::Path};

pub struct File {
    file: std::fs::File,
    ring: rio::Rio,
}

impl File {
    fn open(
        path: impl AsRef<Path>,
        opts: std::fs::OpenOptions,
        ring: rio::Rio,
    ) -> io::Result<Self> {
        let file = opts.open(path)?;
        Ok(Self { file, ring })
    }

    pub fn into_file(self) -> std::fs::File {
        self.file
    }

    pub async fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.ring.read_at(&self.file, &buf, offset).await
    }

    pub async fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        let read_size = self.ring.read_at(&self.file, &buf, offset).await?;
        if read_size != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        Ok(())
    }

    pub async fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.ring.write_at(&self.file, &buf, offset).await
    }

    pub async fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        let write_size = self.ring.write_at(&self.file, &buf, offset).await?;
        if write_size != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short write happened",
            ));
        }
        Ok(())
    }

    pub async fn sync_range(&self, offset: u64, len: usize) -> io::Result<()> {
        self.ring.sync_file_range(&self.file, offset, len).await
    }

    pub async fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

#[derive(Clone)]
pub struct IoManager {
    ring: rio::Rio,
}

impl IoManager {
    pub fn new() -> io::Result<Self> {
        let mut builder = rio::Config::default();
        builder.depth = 4096;

        let ring = builder.start()?;
        Ok(Self { ring })
    }

    pub fn open_file(
        &self,
        path: impl AsRef<Path>,
        opts: std::fs::OpenOptions,
    ) -> io::Result<File> {
        File::open(path, opts, self.ring.clone())
    }

    pub fn open_file_from_fd(&self, file: std::fs::File) -> File {
        File {
            file,
            ring: self.ring.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;

    use super::IoManager;

    #[test]
    fn test_read_and_write() -> anyhow::Result<()> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .build()?;

        let future = async || -> anyhow::Result<()> {
            let center = IoManager::new()?;
            let file = tempfile()?;

            let file = center.open_file_from_fd(file);

            let expected = b"hello world".to_vec();
            file.write_all_at(&expected, 0).await?;
            file.sync_range(0, expected.len()).await?;

            let mut actual = vec![0; expected.len()];
            file.read_exact_at(&mut actual[..], 0).await?;

            assert_eq!(expected, actual);

            Ok(())
        };

        rt.block_on(async {
            future().await.unwrap();
        });

        Ok(())
    }
}
