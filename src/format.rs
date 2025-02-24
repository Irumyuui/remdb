use bytes::{Buf, BufMut, Bytes, BytesMut};

pub fn get_user_key(internal_key: &[u8]) -> &[u8] {
    assert!(internal_key.len() >= 8);
    internal_key[..internal_key.len() - 8].as_ref()
}

pub fn get_key_seq(internal_key: &[u8]) -> u64 {
    assert!(internal_key.len() >= 8);
    internal_key[internal_key.len() - 8..].as_ref().get_u64_le()
}

pub fn make_internal_key(user_key: &[u8], seq: u64) -> Bytes {
    let mut buf = BytesMut::with_capacity(user_key.len() + size_of::<u64>());
    buf.put(user_key);
    buf.put_u64_le(seq);
    buf.freeze()
}
