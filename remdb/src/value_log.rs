#![allow(unused)]

use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    mem,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_channel::Sender;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use fast_async_mutex::{mutex::Mutex, rwlock::RwLock};
use itertools::Itertools;

use crate::{
    batch::{WriteEntry, WriteRequest},
    core::DBInner,
    error::{KvError, KvResult},
    format::{VLOF_FILE_SUFFIX, key::KeyBytes, value::ValuePtr, vlog_format_path},
    fs::File,
    kv_iter::Peekable,
    options::DBOptions,
    table::{Table, table_iter::TableConcatIter},
};

// meta on value first byte
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Header {
    pub(crate) seq: u64,
    key_len: u32,
    value_len: u32,
}

const VLOG_HEADER_SIZE: usize =
    mem::size_of::<u64>() + mem::size_of::<u32>() + mem::size_of::<u32>();

impl Header {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u64_le(self.seq);
        buf.put_u32_le(self.key_len);
        buf.put_u32_le(self.value_len);
    }

    pub fn decode(mut buf: &[u8]) -> KvResult<Self> {
        if buf.len() <= VLOG_HEADER_SIZE {
            return Err(KvError::Decode(format!(
                "header size is not enough: {}",
                buf.len()
            )));
        }

        let seq = buf.get_u64_le();
        let key_len = buf.get_u32_le();
        let value_len = buf.get_u32_le();

        Ok(Self {
            seq,
            key_len,
            value_len,
        })
    }
}

/// Value log file just readonly, write only append.
///
/// ```text
///     +---------+
///     | entry 1 |
///     +---------+
///     | entry 2 |
///     +---------+
///     | ...     |
///     +---------+
///     | entry n |
///     +---------+
/// ```
pub struct ValueLogFile {
    fid: u32,
    file: File,
    write_offset: u64,
    buf: BytesMut, // for wal
}

impl ValueLogFile {
    pub async fn open(fid: u32, file: File) -> KvResult<Self> {
        let write_offset = file.len().await?;

        Ok(Self {
            fid,
            file,
            write_offset,
            buf: BytesMut::with_capacity(VLOG_HEADER_SIZE),
        })
    }

    pub async fn put(&mut self, entry: Entry) -> KvResult<ValuePtr> {
        let ptr = self.put_batch(&[entry]).await?;
        assert!(ptr.len() == 1);
        Ok(ptr.into_iter().next().unwrap())
    }

    // need split buf?
    pub async fn put_batch(&mut self, entries: &[Entry]) -> KvResult<Vec<ValuePtr>> {
        let mut ptrs = Vec::with_capacity(entries.len());
        self.buf.clear();

        let mut write_bytes = 0;
        for e in entries {
            let len = e.encode(&mut self.buf);
            let ptr = ValuePtr {
                fid: self.fid,
                len: len as u32,
                offset: self.write_offset + write_bytes,
            };
            write_bytes += len as u64;
            ptrs.push(ptr);
        }

        self.file.write_all_at(&self.buf, self.write_offset).await?;
        self.write_offset += write_bytes;

        Ok(ptrs)
    }

    pub fn into_file(self) -> File {
        self.file
    }

    pub async fn read_entry(&self, vptr: &ValuePtr) -> KvResult<Bytes> {
        let read_start = vptr.offset();
        let read_end = read_start + vptr.len() as u64;

        if read_end > self.write_offset {
            return Err(KvError::Corruption(
                format!(
                    "vptr offset {} is larger than current write offset {}",
                    read_end, self.write_offset
                )
                .into(),
            ));
        }

        let mut buf = BytesMut::zeroed(vptr.len() as usize);
        self.file.read_exact_at(&mut buf, read_start).await?;
        Ok(buf.freeze())
    }

    async fn read_all_entries(&self) -> KvResult<Vec<Entry>> {
        // TODO: split read

        let mut buf = BytesMut::zeroed(self.write_offset as usize);
        self.file.read_exact_at(&mut buf, 0).await?;

        let buf = buf.freeze();
        let mut entries = Vec::new();
        let mut offset = 0;
        while offset < buf.len() {
            let entry = Entry::decode_from_bytes(buf.slice(offset..))?;
            offset += entry.encode_len();
            entries.push(entry);
        }

        Ok(entries)
    }
}

// TODO: add deleted vlog file, just deleted?
pub struct ValueLogInner {
    achive_vlogs: BTreeMap<u32, Arc<RwLock<ValueLogFile>>>,
    // deleted_vlogs: BTreeMap<u32, Arc<RwLock<ValueLogFile>>>,
    max_fid: u32,
}

impl ValueLogInner {
    fn new() -> Self {
        Self {
            achive_vlogs: BTreeMap::new(),
            // deleted_vlogs: BTreeMap::new(),
            max_fid: 0, // from vlogs dir
        }
    }

    fn current_write_file(&self) -> Arc<RwLock<ValueLogFile>> {
        self.achive_vlogs
            .get(&self.max_fid)
            .expect("max fid must exists")
            .clone()
    }

    fn current_write_fid(&self) -> u32 {
        self.max_fid
    }
}

// TODO: garbage collection
pub struct ValueLog {
    do_gc: Arc<Mutex<()>>,
    inner: Arc<RwLock<ValueLogInner>>,
    options: Arc<DBOptions>,

    deleted_vlogs: RwLock<BTreeMap<u32, Arc<RwLock<ValueLogFile>>>>,

    write_offset: AtomicU64,
}

/// Entry 的存储格式如下
///
/// ```text
///     +----------------+
///     | seq: u64       |
///     +----------------+
///     | key len: u32   |
///     +----------------+
///     | value len: u32 |
///     +----------------+
///     | key            |
///     +----------------+
///     | value          |
///     +----------------+
///     | check sum: u32 |
///     +-----------------
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub(crate) header: Header,
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

impl Entry {
    pub fn new(seq: u64, key: Bytes, value: Bytes) -> Self {
        // TODO: check len
        Self {
            header: Header {
                seq,
                key_len: key.len() as u32,
                value_len: value.len() as u32,
            },
            key,
            value,
        }
    }

    fn decode_from_bytes(entry_bytes: Bytes) -> KvResult<Self> {
        if entry_bytes.len() < VLOG_HEADER_SIZE + 4 {
            return Err(KvError::Corruption(
                format!("entry size {} is too small", entry_bytes.len()).into(),
            ));
        }

        let excepted_crc32 = crc32fast::hash(&entry_bytes[..entry_bytes.len() - 4]);
        let actual_crc32 = entry_bytes[entry_bytes.len() - 4..].as_ref().get_u32_le();
        if excepted_crc32 != actual_crc32 {
            return Err(KvError::ChecksumMismatch);
        }

        let header = Header::decode(&entry_bytes)?;
        let key = entry_bytes.slice(VLOG_HEADER_SIZE..VLOG_HEADER_SIZE + header.key_len as usize);
        let value = entry_bytes.slice(
            VLOG_HEADER_SIZE + header.key_len as usize
                ..VLOG_HEADER_SIZE + header.key_len as usize + header.value_len as usize,
        );
        assert_eq!(
            VLOG_HEADER_SIZE + header.key_len as usize + header.value_len as usize + 4,
            entry_bytes.len()
        );

        Ok(Entry { header, key, value })
    }

    fn encode(&self, buf: &mut BytesMut) -> usize {
        let l = buf.len();
        let mut hasher = crc32fast::Hasher::new();

        self.header.encode(buf);
        hasher.update(&buf[l..]);
        buf.put(self.key.as_ref());
        hasher.update(self.key.as_ref());
        buf.put(self.value.as_ref());
        hasher.update(self.value.as_ref());
        buf.put_u32_le(hasher.finalize());

        buf.len() - l
    }

    pub fn encode_len(&self) -> usize {
        VLOG_HEADER_SIZE + self.key.len() + self.value.len() + 4
    }
}

pub struct Request {
    entries: Vec<Entry>,
    pub(crate) value_ptrs: Vec<ValuePtr>,
}

impl Request {
    pub fn new(entries: Vec<Entry>) -> Self {
        Self {
            entries,
            value_ptrs: Vec::new(),
        }
    }
}

impl ValueLog {
    pub async fn new(options: Arc<DBOptions>) -> KvResult<Self> {
        let mut this = Self {
            do_gc: Arc::new(Mutex::new(())),
            inner: Arc::new(RwLock::new(ValueLogInner::new())),
            options,

            deleted_vlogs: RwLock::new(BTreeMap::new()),

            write_offset: AtomicU64::new(0),
        };

        this.restart_value_log().await?;

        Ok(this)
    }

    async fn restart_value_log(&self) -> KvResult<()> {
        self.open_value_log_dir().await?;
        if !self.restart_as_old_vlog_file().await? {
            self.create_vlog_file().await?;
        }
        Ok(())
    }

    async fn open_value_log_dir(&self) -> KvResult<()> {
        // search all value log files
        tracing::debug!("open value log dir: {:?}", self.options.value_log_dir);

        let dir = std::fs::read_dir(&self.options.value_log_dir)?;
        let mut inner = self.inner.write().await;
        for file in dir {
            tracing::debug!("try open value log file: {:?}", file);

            let file = file?;
            let file_name = file.file_name().into_string().map_err(|e| {
                KvError::Corruption(format!("invalid value log name: {:?}", e).into())
            })?;
            if !file_name.ends_with(VLOF_FILE_SUFFIX) {
                continue;
            }

            let fid = file_name[..file_name.len() - VLOF_FILE_SUFFIX.len()]
                .parse::<u32>()
                .map_err(|e| {
                    KvError::Corruption(
                        format!("invalid value log name, parse fid failed: {:?}", e).into(),
                    )
                })?;

            let file_path = file.path();
            let mut open_options = std::fs::OpenOptions::new();
            open_options.create(false).read(true).write(true);
            let file = self.options.io_manager.open_file(file_path, open_options)?;
            let vlog_file = Arc::new(RwLock::new(ValueLogFile::open(fid, file).await?));

            if inner.achive_vlogs.insert(fid, vlog_file).is_some() {
                return Err(KvError::Corruption(
                    format!("fid {} already exists", fid).into(),
                ));
            }
            inner.max_fid = inner.max_fid.max(fid);
        }

        Ok(())
    }

    async fn restart_as_old_vlog_file(&self) -> KvResult<bool> {
        let inner = self.inner.read().await;
        if let Some(last_file) = inner.achive_vlogs.get(&inner.max_fid) {
            let file = last_file.read().await;
            if file.write_offset < self.options.value_log_size_threshold {
                self.write_offset.store(file.write_offset, Ordering::SeqCst);
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn create_vlog_file(&self) -> KvResult<()> {
        let mut inner = self.inner.write().await;
        let next_fid = inner.max_fid + 1;
        let path = vlog_format_path(&self.options.value_log_dir, next_fid);
        let mut opts = OpenOptions::new();
        opts.create(true).read(true).write(true);
        tracing::debug!("create new vlog file: {:?}, fid: {:?}", path, next_fid);
        let fd = self.options.io_manager.open_file(path, opts)?;
        let vlog_file = Arc::new(RwLock::new(ValueLogFile::open(next_fid, fd).await?));
        assert!(inner.achive_vlogs.insert(next_fid, vlog_file).is_none());
        inner.max_fid = next_fid;
        self.set_current_write_offset(0);
        Ok(())
    }

    fn current_write_offset(&self) -> u64 {
        self.write_offset.load(Ordering::SeqCst)
    }

    fn set_current_write_offset(&self, offset: u64) {
        self.write_offset.store(offset, Ordering::SeqCst);
    }

    async fn get_vlog_file(&self, vptr: &ValuePtr) -> KvResult<Arc<RwLock<ValueLogFile>>> {
        let inner = self.inner.read().await;

        let try_get = |vlogs: &BTreeMap<u32, Arc<RwLock<ValueLogFile>>>| -> KvResult<Arc<RwLock<ValueLogFile>>> {
            if let Some(vlog) = vlogs.get(&vptr.fid()) {
                let max_fid = inner.max_fid;
                if vptr.fid() == max_fid && vptr.offset() >= self.current_write_offset() {
                    Err(KvError::Corruption(
                        format!(
                            "vptr offset {} is larger than current write offset {}",
                            vptr.offset(),
                            self.current_write_offset()
                        )
                        .into(),
                    ))
                } else {
                    Ok(vlog.clone())
                }
            } else {
                Err(KvError::Corruption(
                    format!("fid {} not found", vptr.fid()).into(),
                ))
            }
        };

        match try_get(&inner.achive_vlogs) {
            Ok(res) => Ok(res),
            Err(_) => try_get(&*self.deleted_vlogs.read().await),
        }
    }

    pub async fn read_entry(&self, vptr: ValuePtr) -> KvResult<Entry> {
        tracing::debug!("read value ptr: {:?}", vptr);
        let vlog_file = self.get_vlog_file(&vptr).await?;
        let entry_bytes = { vlog_file.read().await.read_entry(&vptr).await? };
        Entry::decode_from_bytes(entry_bytes)
    }

    // TODO: 让出所有权的参数方式？
    pub async fn write_requests(&self, reqs: &mut [Request]) -> KvResult<()> {
        self.write_inner(reqs).await?;
        Ok(())
    }

    async fn write_inner(&self, reqs: &mut [Request]) -> KvResult<()> {
        let (current_write_fid, current_write_vlog_file) = {
            let inner = self.inner.read().await;
            (inner.current_write_fid(), inner.current_write_file())
        };

        let do_write = async |req: &mut Request, current: &RwLock<ValueLogFile>| -> KvResult<()> {
            let mut ptrs = current_write_vlog_file
                .write()
                .await
                .put_batch(&req.entries)
                .await?;
            req.value_ptrs.append(&mut ptrs);
            Ok(())
        };

        for req in reqs.iter_mut() {
            // TODO: use task?
            let write_bytes = req
                .entries
                .iter()
                .map(|e| e.encode_len() as u64)
                .sum::<u64>();
            do_write(req, &current_write_vlog_file).await?;
            self.write_offset.fetch_add(write_bytes, Ordering::SeqCst);

            if self.should_create_new_vlog_file() {
                self.create_vlog_file().await?;
            }
        }

        if self.should_create_new_vlog_file() {
            self.create_vlog_file().await?;
        }

        Ok(())
    }

    fn should_create_new_vlog_file(&self) -> bool {
        self.current_write_offset() > self.options.value_log_size_threshold
    }

    /// Take the oldest vlog file and rewrite into lsm tree, and return the file id. \
    /// Should be called by user, **if user want to trigger gc manually**.
    pub async fn do_gc(
        &self,
        inner: &DBInner,
        write_req_sender: Sender<WriteRequest>,
    ) -> KvResult<Option<u32>> {
        let core = inner.core.read().await.clone();
        let _gc_lock = self.do_gc.lock().await;

        let (rewirte_file_id, rewrite_file) = {
            let inner = self.inner.read().await;
            if let Some((id, file)) = inner.achive_vlogs.iter().next()
                && *id != inner.max_fid
            {
                (*id, file.clone())
            } else {
                tracing::info!("trigger a vlog gc, but vlog is not need gc");
                return Ok(None);
            }
        };

        let mut remaining_entries = Vec::new();
        let mut l0_iters = Vec::with_capacity(core.ssts[0].len());
        for id in core.ssts[0].iter() {
            let iter = core.ssts_map[id].iter().await?;
            l0_iters.push(iter);
        }
        let mut leveled_iters = Vec::with_capacity(core.ssts.len() - 1);
        for ids in core.ssts.iter().skip(1) {
            let concat_iter =
                TableConcatIter::new(ids.iter().map(|id| core.ssts_map[id].clone()).collect_vec());
            leveled_iters.push(concat_iter);
        }

        let mut search_key = async |key: KeyBytes| -> KvResult<Option<(Arc<Table>, u64)>> {
            // search from level 0
            for iter in l0_iters.iter_mut() {
                iter.seek_to_key(key.as_key_slice()).await?;
                if iter.peek().is_some_and(|item| item.key == key) {
                    let owner_table = iter.table();
                    let value_offset = iter.current_value_offset(); // MUST VALUE PTR, is it need check?
                    return Ok(Some((owner_table, value_offset)));
                }
            }

            // search from lower level
            for iter in leveled_iters.iter_mut() {
                iter.seek_to_key(key.as_key_slice()).await?;
                if iter.peek().is_some_and(|item| item.key == key) {
                    let (offset, table) = iter.value_offset_with_table();
                    return Ok(Some((table, offset)));
                }
            }

            Ok(None)
        };

        for entry in rewrite_file.read().await.read_all_entries().await? {
            let key = KeyBytes::new(entry.key.clone(), entry.header.seq);
            if let Some((table, offset)) = search_key(key).await? {
                remaining_entries.push((table, offset, entry)); // rewrite table, rewrite offset, rewrite entry
            }
        }

        let write_entries = remaining_entries
            .into_iter()
            .map(|(_, _, entry)| WriteEntry {
                key: KeyBytes::new(entry.key, entry.header.seq),
                value: entry.value,
            })
            .collect_vec();
        let (write_req, res_receiver) = WriteRequest::new_batch(write_entries);

        write_req_sender.send(write_req).await.map_err(|e| {
            KvError::Corruption(format!("gc send write request error: {}", e).into())
        })?;
        res_receiver
            .recv()
            .await
            .map_err(|e| KvError::Corruption(format!("gc write request error: {:?}", e).into()))?;

        self.inner
            .write()
            .await
            .achive_vlogs
            .remove(&rewirte_file_id)
            .expect("vlog file not found");

        let res = self
            .deleted_vlogs
            .write()
            .await
            .insert(rewirte_file_id, rewrite_file);
        assert!(res.is_none());

        Ok(Some(rewirte_file_id))
    }

    pub async fn remove_deleted_vlog_file(&self, fid: u32) {
        let result = self.deleted_vlogs.write().await.remove(&fid);
        assert!(result.is_some());
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use bytes::{BufMut, Bytes, BytesMut};
    use itertools::Itertools;
    use tempfile::tempdir;

    use crate::{
        format::key::KeySlice,
        fs::IoManager,
        options::DBOpenOptions,
        test_utils::run_async_test,
        value_log::{Entry, Request, ValueLog},
    };

    use super::ValueLogFile;

    #[test]
    fn test_entry_encode() -> anyhow::Result<()> {
        let entry = Entry::new(
            114514,
            Bytes::copy_from_slice(b"hello"),
            Bytes::copy_from_slice(b"world"),
        );

        let mut buf = BytesMut::new();
        let data = entry.encode(&mut buf);

        let bytes = buf.freeze();
        assert_eq!(data, bytes.len());
        assert_eq!(data, entry.encode_len());

        let decode_entry = Entry::decode_from_bytes(bytes)?;
        assert_eq!(entry.header.seq, decode_entry.header.seq);
        assert_eq!(entry.header.key_len, decode_entry.header.key_len);
        assert_eq!(entry.header.value_len, decode_entry.header.value_len);
        assert_eq!(entry.key, decode_entry.key);
        assert_eq!(entry.value, decode_entry.value);

        Ok(())
    }

    #[test]
    fn test_write_some_data() -> anyhow::Result<()> {
        run_async_test(async || -> anyhow::Result<()> {
            let tempfile = tempfile::tempfile()?;

            let context = IoManager::new()?;
            let file = context.open_file_from_fd(tempfile);
            let fid = 0;

            let mut vlog = ValueLogFile::open(fid, file).await?;

            let keys = ["key1", "key2", "key3"];
            let values = ["value1", "value2", "value3"];
            let entries = keys
                .iter()
                .zip(values.iter())
                .enumerate()
                .map(|(i, (key, value))| {
                    Entry::new(
                        i as u64,
                        Bytes::copy_from_slice(key.as_bytes()),
                        Bytes::copy_from_slice(value.as_bytes()),
                    )
                })
                .collect::<Vec<_>>();

            let ptrs = vlog.put_batch(&entries).await?;

            let file = vlog.into_file();
            let actual = {
                let len = file.len().await? as usize;
                let mut buf = vec![0; len];
                file.read_exact_at(&mut buf, 0).await?;
                buf
            };

            let expected = {
                let mut buf: Vec<u8> = vec![];
                for (i, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
                    let header = super::Header {
                        seq: i as u64,
                        key_len: key.len() as u32,
                        value_len: value.len() as u32,
                    };

                    let mut hasher = crc32fast::Hasher::new();
                    header.encode(&mut buf);
                    hasher.update(&buf[buf.len() - super::VLOG_HEADER_SIZE..]);
                    buf.extend_from_slice(key.as_bytes());
                    hasher.update(key.as_bytes());
                    buf.extend_from_slice(value.as_bytes());
                    hasher.update(value.as_bytes());

                    let checksum = hasher.finalize();
                    buf.put_u32_le(checksum);
                }
                buf
            };

            assert_eq!(expected, actual);

            Ok(())
        })
    }

    #[test]
    fn test_vlog_mgr_read_and_write_basic() -> anyhow::Result<()> {
        run_async_test(async || -> anyhow::Result<()> {
            let temp_dir = tempdir()?;
            let opt = DBOpenOptions::default()
                .value_log_dir(temp_dir.path())
                .build()?;
            let vlog_mgr = ValueLog::new(opt).await?;

            let keys = ["key1", "key2", "key3"];
            let values = ["value1", "value2", "value3"];
            let entries = keys
                .iter()
                .zip(values.iter())
                .enumerate()
                .map(|(i, (key, value))| {
                    Entry::new(
                        i as u64,
                        Bytes::copy_from_slice(key.as_bytes()),
                        Bytes::copy_from_slice(value.as_bytes()),
                    )
                })
                .collect_vec();

            let mut reqs = Request::new(entries.clone());
            let mut reqs = vec![reqs];
            vlog_mgr.write_requests(&mut reqs).await?;

            let ptrs = reqs[0].value_ptrs.clone();
            for (i, ptr) in ptrs.iter().enumerate() {
                let excepted = entries[i].clone();
                let actual = vlog_mgr.read_entry(*ptr).await?;
                assert_eq!(excepted, actual);
            }

            Ok(())
        })
    }

    #[test]
    fn test_vlog_split_file() -> anyhow::Result<()> {
        run_async_test(async || -> anyhow::Result<()> {
            let temp_dir = tempdir()?;
            let opt = DBOpenOptions::default()
                .value_log_dir(temp_dir.path())
                .build()?;
            let vlog_mgr = ValueLog::new(opt).await?;

            const GROUP_ITEM_COUNT: usize = 10;
            const GROUP_COUNT: usize = 10;

            fn gen_group_entry(offset: usize, range: Range<usize>) -> Vec<Entry> {
                range
                    .map(|i| {
                        Entry::new(
                            (i + offset) as u64,
                            Bytes::copy_from_slice(format!("key{}", i).as_bytes()),
                            Bytes::copy_from_slice(format!("value{}", i).as_bytes()),
                        )
                    })
                    .collect_vec()
            }

            let mut entries = vec![vec![]; GROUP_COUNT];
            for (i, list) in entries.iter_mut().enumerate() {
                *list = gen_group_entry(i * GROUP_ITEM_COUNT, 0..GROUP_ITEM_COUNT);
            }

            let mut reqs = entries
                .iter()
                .map(|es| Request::new(es.clone()))
                .collect_vec();
            // vlog_mgr.write_requests(&mut reqs).await?;

            let mut res = vec![];
            for req in reqs {
                let mut reqs = vec![req];
                vlog_mgr.write_requests(&mut reqs[..]).await?;
                res.append(&mut reqs);
                vlog_mgr.create_vlog_file().await?;
            }
            let reqs = res;

            for req in reqs.into_iter() {
                for (i, ptr) in req.value_ptrs.iter().enumerate() {
                    let excepted = req.entries[i].clone();
                    let actual = vlog_mgr.read_entry(*ptr).await?;
                    assert_eq!(excepted, actual);
                }
            }

            Ok(())
        })
    }
}
