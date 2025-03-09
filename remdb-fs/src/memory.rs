use std::{
    collections::HashMap,
    io::{self, SeekFrom},
    path::{Component, Path},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use parking_lot::RwLock;

use crate::traits::{File, FileSystem};

pub mod prelude {
    pub use super::{MemFile, MemFileSystem};
}

#[derive(Debug)]
struct Inode {
    data: RwLock<Vec<u8>>,
    locked: AtomicBool,
}

#[derive(Debug, Default)]
struct DirNode {
    entries: RwLock<HashMap<String, Node>>,
}

#[derive(Debug)]
enum Node {
    File(Arc<Inode>),
    Dir(Arc<DirNode>),
}

#[derive(Debug, Default)]
pub struct MemFileSystem {
    root: Arc<DirNode>,
}

pub struct MemFile {
    inode: Arc<Inode>,
    pos: u64,
}

impl File for MemFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let data = self.inode.data.read();

        let remaining = data.len().saturating_sub(self.pos as usize);
        if remaining == 0 {
            return Ok(0);
        }

        let read_len = buf.len().min(remaining);
        let pos = self.pos as usize;
        buf[..read_len].copy_from_slice(&data[pos..pos + read_len]);
        self.pos += read_len as u64;
        Ok(read_len)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let data = self.inode.data.read();

        let start = self.pos as usize;
        buf.extend_from_slice(&data[start..]);
        let read = data.len() - start;
        self.pos = data.len() as u64;
        Ok(read)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let data = self.inode.data.read();

        let offset = offset as usize;
        if offset >= data.len() {
            return Ok(0);
        }

        let remaining = data.len() - offset;
        let read_len = buf.len().min(remaining);
        buf[..read_len].copy_from_slice(&data[offset..offset + read_len]);
        Ok(read_len)
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut data = self.inode.data.write();

        let pos = self.pos as usize;
        let new_len = pos + buf.len();
        if data.len() < new_len {
            data.resize(new_len, 0);
        }

        data[pos..new_len].copy_from_slice(buf);
        self.pos = new_len as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn close(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                let len = self.inode.data.read().len() as i64;
                let new_pos = len + offset;
                if new_pos < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Negative seek position",
                    ));
                }
                new_pos as u64
            }

            SeekFrom::Current(offset) => {
                let current = self.pos as i64;
                let new_pos = current + offset;
                if new_pos < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Negative seek position",
                    ));
                }
                new_pos as u64
            }
        };

        self.pos = new_pos;
        Ok(new_pos)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.inode.data.read().len() as u64)
    }

    fn is_empty(&self) -> bool {
        self.inode.data.read().is_empty()
    }

    fn lock(&self) -> io::Result<()> {
        if self
            .inode
            .locked
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Acquire)
            .is_err()
        {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "File already locked",
            ))
        } else {
            Ok(())
        }
    }

    fn unlock(&self) -> io::Result<()> {
        self.inode.locked.store(false, Ordering::Release);
        Ok(())
    }
}

impl FileSystem for MemFileSystem {
    type File = MemFile;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File> {
        let path = path.as_ref();
        let (parent, name) = split_parent_and_name(path)?;
        let parent_dir = self.get_dir(parent)?;

        let mut entries = parent_dir.entries.write();
        if let Some(existing) = entries.get(name) {
            match existing {
                Node::File(inode) => {
                    let mut data = inode.data.write();
                    data.clear();
                    return Ok(MemFile {
                        inode: inode.clone(),
                        pos: 0,
                    });
                }
                Node::Dir(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "Path is a directory",
                    ));
                }
            }
        }

        let inode = Arc::new(Inode {
            data: RwLock::new(Vec::new()),
            locked: AtomicBool::new(false),
        });
        entries.insert(name.to_owned(), Node::File(inode.clone()));
        Ok(MemFile { inode, pos: 0 })
    }

    fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File> {
        let path = path.as_ref();
        let (parent, name) = split_parent_and_name(path)?;
        let parent_dir = self.get_dir(parent)?;

        let entries = parent_dir.entries.read();

        match entries.get(name) {
            Some(Node::File(inode)) => Ok(MemFile {
                inode: inode.clone(),
                pos: 0,
            }),
            Some(Node::Dir(_)) => Err(io::Error::new(io::ErrorKind::Other, "Path is a directory")),
            None => Err(io::Error::new(io::ErrorKind::NotFound, "File not found")),
        }
    }

    fn remove<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        let (parent, name) = split_parent_and_name(path)?;
        let parent_dir = self.get_dir(parent)?;

        let mut entries = parent_dir.entries.write();

        match entries.get(name) {
            Some(Node::File(_)) => {
                entries.remove(name);
                Ok(())
            }
            Some(Node::Dir(_)) => Err(io::Error::new(
                io::ErrorKind::Other,
                "Can't remove directory with remove()",
            )),
            None => Err(io::Error::new(io::ErrorKind::NotFound, "File not found")),
        }
    }

    fn mkdir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        let (parent, name) = split_parent_and_name(path)?;
        let parent_dir = self.get_dir(parent)?;

        let mut entries = parent_dir.entries.write();

        if entries.contains_key(name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "Directory exists",
            ));
        }

        entries.insert(
            name.to_owned(),
            Node::Dir(Arc::new(DirNode {
                entries: RwLock::new(HashMap::new()),
            })),
        );
        Ok(())
    }

    fn mkdir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        let components = path
            .components()
            .filter(|c| matches!(c, Component::Normal(_)))
            .map(|c| c.as_os_str().to_str().unwrap());

        let mut current_dir = self.root.clone();
        for component in components {
            let next_dir = {
                let mut entries = current_dir.entries.write();

                match entries.get(component) {
                    Some(Node::Dir(dir)) => dir.clone(),
                    Some(Node::File(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Path component is a file",
                        ));
                    }
                    None => {
                        let new_dir = Arc::new(DirNode {
                            entries: RwLock::new(HashMap::new()),
                        });
                        entries.insert(component.to_owned(), Node::Dir(new_dir.clone()));
                        new_dir
                    }
                }
            };
            current_dir = next_dir;
        }
        Ok(())
    }

    fn remove_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        let path = path.as_ref();
        let (parent, name) = split_parent_and_name(path)?;
        let parent_dir = self.get_dir(parent)?;

        let dir_arc = {
            let entries = parent_dir.entries.read();

            match entries.get(name) {
                Some(Node::Dir(dir)) => dir.clone(),
                Some(Node::File(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Path is not a directory",
                    ));
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "Directory not found",
                    ));
                }
            }
        };

        let is_empty = {
            let entries = dir_arc.entries.read();
            entries.is_empty()
        };

        if !is_empty {
            return Err(io::Error::new(io::ErrorKind::Other, "Directory not empty"));
        }

        let mut entries = parent_dir.entries.write();
        entries.remove(name);
        Ok(())
    }

    fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
        let path = path.as_ref();
        if path == Path::new("/") {
            return true;
        }

        let Ok((parent, name)) = split_parent_and_name(path) else {
            return false;
        };

        let Ok(parent_dir) = self.get_dir(parent) else {
            return false;
        };

        parent_dir.entries.read().contains_key(name)
    }
}

fn split_parent_and_name(path: &Path) -> io::Result<(&Path, &str)> {
    let mut components = path.components();

    let name = components
        .next_back()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Empty path"))?;

    let name = match name {
        Component::Normal(n) => n
            .to_str()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Invalid filename"))?,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid path component",
            ));
        }
    };

    let parent = components.as_path();
    Ok((parent, name))
}

impl MemFileSystem {
    fn get_dir(&self, path: &Path) -> io::Result<Arc<DirNode>> {
        let components = path
            .components()
            .filter(|c| matches!(c, Component::Normal(_)))
            .map(|c| {
                c.as_os_str()
                    .to_str()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Invalid component"))
            });

        let mut current_dir = self.root.clone();
        for component in components {
            let component = component?;
            let next_dir = {
                let entries = current_dir.entries.read();

                match entries.get(component) {
                    Some(Node::Dir(dir)) => dir.clone(),
                    Some(Node::File(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            "Path component is a file",
                        ));
                    }
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "Directory not found",
                        ));
                    }
                }
            };
            current_dir = next_dir;
        }
        Ok(current_dir)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_file_operations() {
        let fs = MemFileSystem::default();

        let mut file = fs.create("/test.txt").unwrap();
        assert_eq!(file.write(b"Hello").unwrap(), 5);

        let mut buf = [0u8; 5];
        file.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(file.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"Hello");

        assert_eq!(file.write(b" World").unwrap(), 6);
        assert_eq!(file.len().unwrap(), 11);

        drop(file);
        let mut file = fs.open("/test.txt").unwrap();
        let mut content = Vec::new();
        file.read_to_end(&mut content).unwrap();
        assert_eq!(content, b"Hello World");
    }

    #[test]
    fn test_directory_operations() {
        let fs = MemFileSystem::default();

        fs.mkdir_all("/a/b/c").unwrap();
        assert!(fs.exists("/a"));
        assert!(fs.exists("/a/b"));
        assert!(fs.exists("/a/b/c"));

        fs.create("/a/b/c/file.txt").unwrap();
        assert!(fs.exists("/a/b/c/file.txt"));

        fs.remove("/a/b/c/file.txt").unwrap();
        fs.remove_dir("/a/b/c").unwrap();
        assert!(!fs.exists("/a/b/c"));

        fs.create("/a/b/new_file.txt").unwrap();
        assert!(fs.remove_dir("/a/b").is_err());
    }

    #[test]
    fn test_concurrent_access() {
        let fs = Arc::new(MemFileSystem::default());
        let mut handles = vec![];

        for i in 0..10 {
            let fs = fs.clone();
            handles.push(std::thread::spawn(move || {
                let path = format!("/file-{}.txt", i);
                let mut file = fs.create(&path).unwrap();
                file.write(format!("Data {}", i).as_bytes()).unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..10 {
            let path = format!("/file-{}.txt", i);
            assert!(fs.exists(path));
        }
    }

    #[test]
    fn test_some_edge_cases() {
        let fs = MemFileSystem::default();

        assert!(fs.exists("/"));
        assert!(fs.create("").is_err());

        fs.create("/dup.txt").unwrap();
        assert!(fs.create("/dup.txt").is_ok());

        assert!(fs.remove("/ghost.txt").is_err());

        let file = fs.create("/lock.txt").unwrap();
        assert!(file.lock().is_ok());
        assert!(file.lock().is_err());
        assert!(file.unlock().is_ok());
    }

    #[test]
    fn test_seek_operations() {
        let fs = MemFileSystem::default();
        let mut file = fs.create("/seek.txt").unwrap();

        file.write(b"ABCDEFGHIJ").unwrap();

        file.seek(SeekFrom::Start(2)).unwrap();
        let mut buf = [0; 3];
        file.read(&mut buf).unwrap();
        assert_eq!(&buf, b"CDE");

        file.seek(SeekFrom::Current(-2)).unwrap();
        let mut buf = [0; 2];
        file.read(&mut buf).unwrap();
        assert_eq!(&buf, b"DE");

        file.seek(SeekFrom::End(-3)).unwrap();
        let mut buf = [0; 3];
        file.read(&mut buf).unwrap();
        assert_eq!(&buf, b"HIJ");
    }

    #[test]
    fn test_dir_opt() {
        let fs = MemFileSystem::default();

        fs.mkdir_all("/a/b/c").unwrap();
        assert!(fs.exists("/a"));
        assert!(fs.exists("/a/b"));
        assert!(fs.exists("/a/b/c"));

        fs.create("/a/b/c/file.txt").unwrap();
        assert!(fs.exists("/a/b/c/file.txt"));

        fs.remove("/a/b/c/file.txt").unwrap();
        fs.remove_dir("/a/b/c").unwrap();
        assert!(!fs.exists("/a/b/c"));

        fs.create("/a/b/new_file.txt").unwrap();
        assert!(fs.remove_dir("/a/b").is_err());
    }

    #[test]
    fn test_concurrent() {
        let fs = Arc::new(MemFileSystem::default());
        let mut handles = vec![];

        for i in 0..10 {
            let fs = fs.clone();
            handles.push(std::thread::spawn(move || {
                let path = format!("/file-{}.txt", i);
                let mut file = fs.create(&path).unwrap();
                file.write(format!("Data {}", i).as_bytes()).unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..10 {
            let path = format!("/file-{}.txt", i);
            assert!(fs.exists(path));
        }
    }

    #[test]
    fn test_read_at() {
        let fs = MemFileSystem::default();
        let mut file = fs.create("/read_at.txt").unwrap();

        file.write(b"Hello, world!").unwrap();

        let mut buf = [0; 5];
        assert_eq!(file.read_at(&mut buf, 7).unwrap(), 5);
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn test_read_to_end() {
        let fs = MemFileSystem::default();
        let mut file = fs.create("/read_to_end.txt").unwrap();

        file.write(b"Hello, world!").unwrap();

        let mut buf = Vec::new();
        file.seek(SeekFrom::Start(7)).unwrap();
        assert_eq!(file.read_to_end(&mut buf).unwrap(), 6);
        assert_eq!(&buf, b"world!");
    }

    #[test]
    fn test_remove_non_empty_directory() {
        let fs = MemFileSystem::default();

        fs.mkdir_all("/a/b/c").unwrap();
        fs.create("/a/b/c/file.txt").unwrap();

        assert!(fs.remove_dir("/a/b/c").is_err());
    }

    #[test]
    fn test_remove_empty_directory() {
        let fs = MemFileSystem::default();

        fs.mkdir_all("/a/b/c").unwrap();
        fs.remove_dir("/a/b/c").unwrap();
        assert!(!fs.exists("/a/b/c"));
    }

    #[test]
    fn test_create_and_remove_file() {
        let fs = MemFileSystem::default();

        fs.create("/test.txt").unwrap();
        assert!(fs.exists("/test.txt"));

        fs.remove("/test.txt").unwrap();
        assert!(!fs.exists("/test.txt"));
    }

    #[test]
    fn test_create_and_remove_directory() {
        let fs = MemFileSystem::default();

        fs.mkdir("/test_dir").unwrap();
        assert!(fs.exists("/test_dir"));

        fs.remove_dir("/test_dir").unwrap();
        assert!(!fs.exists("/test_dir"));
    }

    #[test]
    fn test_file_read_write() {
        let fs = MemFileSystem::default();
        let mut file = fs.create("/test.txt").unwrap();

        assert_eq!(file.write(b"Hello").unwrap(), 5);

        let mut buf = [0u8; 5];
        file.seek(SeekFrom::Start(0)).unwrap();
        assert_eq!(file.read(&mut buf).unwrap(), 5);
        assert_eq!(&buf, b"Hello");

        assert_eq!(file.write(b" World").unwrap(), 6);
        assert_eq!(file.len().unwrap(), 11);

        drop(file);
        let mut file = fs.open("/test.txt").unwrap();
        let mut content = Vec::new();
        file.read_to_end(&mut content).unwrap();
        assert_eq!(content, b"Hello World");
    }

    #[test]
    fn test_file_read_at() {
        let fs = MemFileSystem::default();
        let mut file = fs.create("/read_at.txt").unwrap();

        file.write(b"Hello, world!").unwrap();

        let mut buf = [0; 5];
        assert_eq!(file.read_at(&mut buf, 7).unwrap(), 5);
        assert_eq!(&buf, b"world");
    }

    #[test]
    fn test_file_read_to_end() {
        let fs = MemFileSystem::default();
        let mut file = fs.create("/read_to_end.txt").unwrap();

        file.write(b"Hello, world!").unwrap();

        let mut buf = Vec::new();
        file.seek(SeekFrom::Start(7)).unwrap();
        assert_eq!(file.read_to_end(&mut buf).unwrap(), 6);
        assert_eq!(&buf, b"world!");
    }
}
