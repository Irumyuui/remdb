use std::fs::File;

pub trait CrossFileExt {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize>;

    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
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

    // fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize>;
}

impl CrossFileExt for File {
    #[cfg(unix)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        use std::os::unix::fs::FileExt;
        FileExt::read_at(self, buf, offset)
    }

    #[cfg(windows)]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        use std::os::windows::fs::FileExt;
        FileExt::seek_read(self, buf, offset)
    }

    #[cfg(not(any(unix, windows)))]
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        unimplemented!()
    }
}
