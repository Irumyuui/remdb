use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::utils::varint::VarInt;

pub fn get_user_key(internal_key: &[u8]) -> &[u8] {
    assert!(internal_key.len() >= 8);
    internal_key[..internal_key.len() - 8].as_ref()
}

pub fn get_key_seq(internal_key: &[u8]) -> u64 {
    assert!(internal_key.len() >= 8);
    internal_key[internal_key.len() - 8..].as_ref().get_u64_le()
}

// read from memtable key
pub fn get_mem_internal_key(memtable_key: &[u8]) -> &[u8] {
    let (key_len, var_int_len): (u32, usize) =
        VarInt::from_varint(&memtable_key).expect("Failed to parsed varint, is it right?");
    memtable_key[var_int_len..var_int_len + key_len as usize].as_ref()
}

pub fn get_mem_value(memtable_key: &[u8]) -> &[u8] {
    let (key_len, var_int_len): (u32, usize) =
        VarInt::from_varint(&memtable_key).expect("Failed to parsed varint, is it right?");

    let key = memtable_key[var_int_len + key_len as usize..].as_ref();
    let (value_len, var_int_len): (u32, usize) =
        VarInt::from_varint(key).expect("Faild to parse value varint");

    let value = key[var_int_len..].as_ref();
    assert_eq!(value.len(), value_len as usize);

    value
}

pub fn get_mem_value_to_bytes(memtable_key: &Bytes) -> Bytes {
    let mut offset = 0;

    let key = memtable_key.as_ref();
    let (key_len, var_int_len): (u32, usize) =
        VarInt::from_varint(&key).expect("Failed to parsed varint, is it right?");
    offset += key_len as usize + var_int_len;

    let key = key[var_int_len + key_len as usize..].as_ref();
    let (value_len, var_int_len): (u32, usize) =
        VarInt::from_varint(key).expect("Faild to parse value varint");

    offset += var_int_len;

    let value = key[var_int_len..].as_ref();
    assert_eq!(value.len(), value_len as usize);
    assert_eq!(memtable_key.len() - offset, value_len as usize);

    memtable_key.slice(offset..)
}

pub fn make_memtable_key(seq: u64, key: &[u8], value: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(size_of::<u64>() + key.len() + value.len() + 10);
    VarInt::put_varint(&((key.len() + size_of::<u64>()) as u32), &mut buf);
    buf.put(key);
    buf.put_u64_le(seq);
    VarInt::put_varint(&(value.len() as u32), &mut buf);
    buf.put(value);
    buf.freeze()
}

pub fn make_lookup_key(seq: u64, key: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(key.len() + 18);
    VarInt::put_varint(&((key.len() + size_of::<u64>()) as u32), &mut buf);
    buf.put(key);
    buf.put_u64_le(seq);
    buf.freeze()
}
