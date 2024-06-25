use std::io::Read;

use crate::macros::{option_get_or_return_none, result_get_or_return_none};

pub fn read_n_bytes(reader: &mut impl Read, n: usize) -> Option<Vec<u8>> {
    let mut buffer = vec![0u8; n];
    result_get_or_return_none!(_unused, reader.read_exact(&mut buffer));
    return Some(buffer);
}

pub fn read_byte(reader: &mut impl Read) -> Option<u8> {
    option_get_or_return_none!(byte_vec, read_n_bytes(reader, 1));
    return Some(byte_vec[0]);
}
