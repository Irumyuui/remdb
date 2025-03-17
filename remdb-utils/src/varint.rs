use bytes::BufMut;

#[derive(Debug, thiserror::Error)]
pub enum VarIntError {
    #[error("insufficient bytes")]
    InsufficientBytes,

    #[error("overflow")]
    Overflow,
}

pub trait VarInt: Sized {
    // return length of the varint
    fn encode_varint(&self, buf: impl AsMut<[u8]>) -> usize;

    fn put_varint(&self, buf: &mut impl BufMut) -> usize;

    fn from_varint(buf: &[u8]) -> Result<(Self, usize), VarIntError>;
}

impl VarInt for u64 {
    fn encode_varint(&self, mut buf: impl AsMut<[u8]>) -> usize {
        let mut n: u64 = *self;
        let mut buf = buf.as_mut();

        let mut len = 0;
        loop {
            let mut byte = (n as u8) & 0x7F;
            n >>= 7;
            if n != 0 {
                byte |= 0x80;
            }
            buf.put_u8(byte);
            len += 1;
            if n == 0 {
                break;
            }
        }
        len
    }

    fn from_varint(buf: &[u8]) -> Result<(Self, usize), VarIntError> {
        let mut n = 0u64;
        let mut shift = 0;

        let mut i = 0;
        for &byte in buf {
            if shift >= 64 {
                return Err(VarIntError::Overflow);
            }

            n |= ((byte & 0x7F) as u64) << shift;
            shift += 7;
            i += 1;
            if byte & 0x80 == 0 {
                return Ok((n, i));
            }
        }

        Err(VarIntError::InsufficientBytes)
    }

    fn put_varint(&self, buf: &mut impl BufMut) -> usize {
        let mut n: u64 = *self;

        let mut len = 0;
        loop {
            let mut byte = (n as u8) & 0x7F;
            n >>= 7;
            if n != 0 {
                byte |= 0x80;
            }
            buf.put_u8(byte);
            len += 1;
            if n == 0 {
                break;
            }
        }
        len
    }
}

impl VarInt for u32 {
    fn encode_varint(&self, buf: impl AsMut<[u8]>) -> usize {
        let n: u64 = (*self).into();
        n.encode_varint(buf)
    }

    fn put_varint(&self, buf: &mut impl BufMut) -> usize {
        let n: u64 = (*self).into();
        n.put_varint(buf)
    }

    fn from_varint(buf: &[u8]) -> Result<(Self, usize), VarIntError> {
        let (n, len) = u64::from_varint(buf)?;
        let n = u32::try_from(n).map_err(|_| VarIntError::Overflow)?;
        Ok((n, len))
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::varint::VarInt;

    #[test]
    fn test_translate_and_read_u32() {
        let cases = vec![
            (0b0_1111111_u32, Bytes::copy_from_slice(&[0b0111_1111_u8])),
            (0b1_1111111_u32, Bytes::copy_from_slice(&[0xFF, 0x01])),
            (
                0b0010100_0101010_u32,
                Bytes::copy_from_slice(&[0b10101010, 0b00010100]),
            ),
            (
                0b0010000_1010101_0101010_u32,
                Bytes::copy_from_slice(&[0b10101010, 0b11010101, 0b00010000]),
            ),
            (
                0b11111111_11111111_11111111_11111111_u32,
                Bytes::copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
            ),
        ];

        for (value, expected) in cases {
            let mut buf = BytesMut::new();
            let _varint = VarInt::put_varint(&value, &mut buf);
            let buf = buf.freeze();
            assert_eq!(buf, expected);
            let res: u32 = VarInt::from_varint(&buf).unwrap().0;
            assert_eq!(res, res);
        }
    }

    #[test]
    #[should_panic]
    fn test_read_u32_error() {
        let cases = 0xFFFFFFFFFF_u64;
        let mut buf = vec![];
        let _ = VarInt::put_varint(&cases, &mut buf);
        let _res: u32 = VarInt::from_varint(&buf).unwrap().0;
    }

    #[test]
    fn test_trans_and_read_u64() {
        let cases = [
            (0b0_1111111_u64, Bytes::copy_from_slice(&[0b0111_1111_u8])),
            (0b1_1111111_u64, Bytes::copy_from_slice(&[0xFF, 0x01])),
            (
                0b0010100_0101010_u64,
                Bytes::copy_from_slice(&[0b10101010, 0b00010100]),
            ),
            (
                0b0010000_1010101_0101010_u64,
                Bytes::copy_from_slice(&[0b10101010, 0b11010101, 0b00010000]),
            ),
            (
                0b11111111_11111111_11111111_11111111_u64,
                Bytes::copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
            ),
            (
                u64::MAX,
                Bytes::copy_from_slice(&[
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01,
                ]),
            ),
        ];

        for (value, expected) in cases.iter() {
            let mut buf = BytesMut::new();
            let _varint = VarInt::put_varint(value, &mut buf);
            let buf = buf.freeze();
            assert_eq!(buf, *expected);
            let res: u64 = VarInt::from_varint(&buf).unwrap().0;
            assert_eq!(res, *value);
        }
    }

    #[test]
    fn test_try_from_slice() {
        let slice = &[
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0xFF,
        ][..];
        let excepted = u64::MAX;

        let res: u64 = VarInt::from_varint(slice).unwrap().0;
        assert_eq!(res, excepted);
    }
}
