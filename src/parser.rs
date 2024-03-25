use num_bigint::BigInt;
use std::str::FromStr;
use std::{
    // collections::{HashMap, HashSet},
    io::Read,
    str::from_utf8,
};

// #[derive(Eq, Hash, PartialEq)]
#[derive(Debug)]
pub enum RedisType {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RedisType>),
    Null,
    Boolean(bool),
    Double(f64),
    BigNumber(BigInt),
    BulkError(String),
    VerbatimString(String),
    // Map(HashMap<RedisType, RedisType>),
    // Set(HashSet<RedisType>),
    Push(Vec<RedisType>),
}

fn read_n_bytes(reader: &mut impl Read, n: usize) -> Option<Vec<u8>> {
    let mut buffer = vec![0u8; n];
    result_get_or_return_none!(_unused, reader.read_exact(&mut buffer));
    return Some(buffer);
}

fn read_byte(reader: &mut impl Read) -> Option<u8> {
    option_get_or_return_none!(byte_vec, read_n_bytes(reader, 1));
    return Some(byte_vec[0]);
}

fn read_to_next_crlf(reader: &mut impl Read) -> Option<Vec<u8>> {
    let mut result = Vec::new();
    let mut cr_found = false;
    loop {
        option_get_or_return_none!(byte, read_byte(reader));
        match byte {
            b'\r' => {
                cr_found = true;
            }
            b'\n' => {
                if cr_found {
                    return Some(result);
                } else {
                    result.push(byte);
                }
            }
            _ => {
                if cr_found {
                    result.push(b'\r');
                }
                result.push(byte);
                cr_found = false;
            }
        }
    }
}

fn scan_string(reader: &mut impl Read) -> Option<String> {
    option_get_or_return_none!(bytes, read_to_next_crlf(reader));
    result_get_or_return_none!(string, from_utf8(&bytes));
    return Some(string.to_string());
}

fn scan_int(reader: &mut impl Read) -> Option<i64> {
    option_get_or_return_none!(string, scan_string(reader));
    result_get_or_return_none!(integer, str::parse::<i64>(&string));
    return Some(integer);
}

fn bulk_string(reader: &mut impl Read) -> Option<String> {
    option_get_or_return_none!(length, scan_int(reader));
    option_get_or_return_none!(bytes, read_n_bytes(reader, length as usize + 2)); // +2 to get the extra crlf
    result_get_or_return_none!(string, from_utf8(&bytes[..length as usize]));
    return Some(string.to_string());
}

pub fn decode(reader: &mut impl Read) -> Option<RedisType> {
    option_get_or_return_none!(type_byte, read_byte(reader));

    match type_byte {
        // simple strings
        b'+' => {
            option_get_or_return_none!(string, scan_string(reader));
            return Some(RedisType::SimpleString(string));
        }
        // simple errors
        b'-' => {
            option_get_or_return_none!(string, scan_string(reader));
            return Some(RedisType::SimpleError(string));
        }
        // integers
        b':' => {
            option_get_or_return_none!(integer, scan_int(reader));
            return Some(RedisType::Integer(integer));
        }
        // bulk strings
        b'$' => {
            option_get_or_return_none!(string, bulk_string(reader));
            return Some(RedisType::BulkString(string));
        }
        // arrays
        b'*' => {
            option_get_or_return_none!(length, scan_int(reader));
            let mut array: Vec<RedisType> = Vec::with_capacity(length as usize);
            for _ in 0..length {
                option_get_or_return_none!(value, decode(reader));
                array.push(value);
            }
            return Some(RedisType::Array(array));
        }
        // nulls
        b'_' => {
            option_get_or_return_none!(_unused, read_n_bytes(reader, 2)); // skip the crlf for the next decode
            return Some(RedisType::Null);
        }
        // booleans
        b'#' => {
            option_get_or_return_none!(boolean_byte, read_byte(reader));
            match boolean_byte {
                b't' => return Some(RedisType::Boolean(true)),
                b'f' => return Some(RedisType::Boolean(false)),
                _ => return None,
            }
        }
        // doubles
        b',' => {
            option_get_or_return_none!(string, scan_string(reader));
            result_get_or_return_none!(double, str::parse::<f64>(&string));
            return Some(RedisType::Double(double));
        }
        // big numbers
        b'(' => {
            option_get_or_return_none!(string, scan_string(reader));
            result_get_or_return_none!(big_int, BigInt::from_str(&string));
            return Some(RedisType::BigNumber(big_int));
        }
        // bulk errors
        b'!' => {
            option_get_or_return_none!(error, bulk_string(reader));
            return Some(RedisType::BulkError(error));
        }
        // verbatim strings
        b'=' => {
            option_get_or_return_none!(string, bulk_string(reader));
            return Some(RedisType::VerbatimString(string));
        }
        // TODO fix hashing and equals issues on RedisType
        /*// maps
        b'%' => {
            option_get_or_return_none!(size, scan_int(reader));
            let mut map: HashMap<RedisType, RedisType> = HashMap::with_capacity(size as usize);
            for _ in 0..size {
                option_get_or_return_none!(key, decode(reader));
                option_get_or_return_none!(value, decode(reader));
                map.insert(key, value);
            }
            return Some(RedisType::Map(map));
        }
        // sets
        b'~' => {
            option_get_or_return_none!(size, scan_int(reader));
            let mut set: HashSet<RedisType> = HashSet::with_capacity(size as usize);
            for _ in 0..size {
                option_get_or_return_none!(value, decode(reader));
                set.insert(value);
            }
            return Some(RedisType::Set(set));
        }*/
        // pushes
        b'>' => {
            option_get_or_return_none!(length, scan_int(reader));
            let mut array: Vec<RedisType> = Vec::with_capacity(length as usize);
            for _ in 0..length {
                option_get_or_return_none!(value, decode(reader));
                array.push(value);
            }
            return Some(RedisType::Push(array));
        }
        _ => {
            println!("data type {:?} not handled", type_byte);
            return None;
        }
    }
}

pub fn encode_simple_string(string: &str) -> String {
    return "+".to_owned() + string + "\r\n";
}

pub fn encode_simple_error(error: &str) -> String {
    return "-".to_owned() + error + "\r\n";
}
