use num_bigint::BigInt;
use std::str::FromStr;
use std::{
    // collections::{HashMap, HashSet},
    io::Read,
    str::from_utf8,
};

use crate::utils::byte_stream;

// #[derive(Eq, Hash, PartialEq)]
#[derive(Debug)]
pub enum RedisType {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(Option<String>),
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

fn read_to_next_crlf(reader: &mut impl Read) -> Option<(Vec<u8>, u64)> {
    let mut result = Vec::new();
    let mut cr_found = false;
    let mut bytes_read = 0;
    loop {
        option_get_or_return_none!(byte, byte_stream::read_byte(reader));
        bytes_read += 1;
        match byte {
            b'\r' => {
                cr_found = true;
            }
            b'\n' => {
                if cr_found {
                    return Some((result, bytes_read));
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

fn scan_string(reader: &mut impl Read) -> Option<(String, u64)> {
    option_get_or_return_none!(bytes_and_count, read_to_next_crlf(reader));
    let (bytes, bytes_read) = bytes_and_count;
    result_get_or_return_none!(string, from_utf8(&bytes));
    return Some((string.to_string(), bytes_read));
}

fn scan_int(reader: &mut impl Read) -> Option<(i64, u64)> {
    option_get_or_return_none!(string_and_count, scan_string(reader));
    let (string, bytes_read) = string_and_count;
    result_get_or_return_none!(integer, str::parse::<i64>(&string));
    return Some((integer, bytes_read));
}

fn bulk_string(reader: &mut impl Read) -> Option<(Option<String>, u64)> {
    option_get_or_return_none!(length_and_count, scan_int(reader));
    let (length, bytes_read) = length_and_count;
    if length < 0 {
        return Some((None, bytes_read));
    }
    option_get_or_return_none!(
        bytes,
        byte_stream::read_n_bytes(reader, length as usize + 2)
    ); // +2 to get the extra crlf
    result_get_or_return_none!(string, from_utf8(&bytes[..length as usize]));
    return Some((Some(string.to_string()), bytes_read + length as u64 + 2));
}

pub fn decode(reader: &mut impl Read) -> Option<(RedisType, u64)> {
    option_get_or_return_none!(type_byte, byte_stream::read_byte(reader));

    match type_byte {
        // simple strings
        b'+' => {
            option_get_or_return_none!(string_and_count, scan_string(reader));
            let (string, bytes_read) = string_and_count;
            return Some((RedisType::SimpleString(string), bytes_read + 1));
        }
        // simple errors
        b'-' => {
            option_get_or_return_none!(string_and_count, scan_string(reader));
            let (string, bytes_read) = string_and_count;
            return Some((RedisType::SimpleError(string), bytes_read + 1));
        }
        // integers
        b':' => {
            option_get_or_return_none!(integer_and_count, scan_int(reader));
            let (integer, bytes_read) = integer_and_count;
            return Some((RedisType::Integer(integer), bytes_read + 1));
        }
        // bulk strings
        b'$' => {
            option_get_or_return_none!(string_and_count, bulk_string(reader));
            let (string, bytes_read) = string_and_count;
            return Some((RedisType::BulkString(string), bytes_read + 1));
        }
        // arrays
        b'*' => {
            option_get_or_return_none!(length_and_count, scan_int(reader));
            let (length, mut bytes_read) = length_and_count;
            let mut array: Vec<RedisType> = Vec::with_capacity(length as usize);
            for _ in 0..length {
                option_get_or_return_none!(value_and_count, decode(reader));
                let (value, bytes_read_for_value) = value_and_count;
                bytes_read += bytes_read_for_value;
                array.push(value);
            }
            return Some((RedisType::Array(array), bytes_read + 1));
        }
        // nulls
        b'_' => {
            option_get_or_return_none!(_unused, byte_stream::read_n_bytes(reader, 2)); // skip the crlf for the next decode
            return Some((RedisType::Null, 3));
        }
        // booleans
        b'#' => {
            option_get_or_return_none!(boolean_byte, byte_stream::read_byte(reader));
            match boolean_byte {
                b't' => return Some((RedisType::Boolean(true), 4)),
                b'f' => return Some((RedisType::Boolean(false), 4)),
                _ => return None,
            }
        }
        // doubles
        b',' => {
            option_get_or_return_none!(string_and_count, scan_string(reader));
            let (string, bytes_read) = string_and_count;
            result_get_or_return_none!(double, str::parse::<f64>(&string));
            return Some((RedisType::Double(double), bytes_read + 1));
        }
        // big numbers
        b'(' => {
            option_get_or_return_none!(string_and_count, scan_string(reader));
            let (string, bytes_read) = string_and_count;
            result_get_or_return_none!(big_int, BigInt::from_str(&string));
            return Some((RedisType::BigNumber(big_int), bytes_read + 1));
        }
        // bulk errors
        b'!' => {
            option_get_or_return_none!(error_option_and_count, bulk_string(reader));
            let (error_option, bytes_read) = error_option_and_count;
            option_get_or_return_none!(error, error_option);
            return Some((RedisType::BulkError(error), bytes_read + 1));
        }
        // verbatim strings
        b'=' => {
            option_get_or_return_none!(string_option_and_count, bulk_string(reader));
            let (string_option, bytes_read) = string_option_and_count;
            option_get_or_return_none!(string, string_option);
            return Some((RedisType::VerbatimString(string), bytes_read + 1));
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
            option_get_or_return_none!(length_and_count, scan_int(reader));
            let (length, mut bytes_read) = length_and_count;
            let mut array: Vec<RedisType> = Vec::with_capacity(length as usize);
            for _ in 0..length {
                option_get_or_return_none!(value_and_count, decode(reader));
                let (value, bytes_read_for_value) = value_and_count;
                bytes_read += bytes_read_for_value;
                array.push(value);
            }
            return Some((RedisType::Push(array), bytes_read + 1));
        }
        _ => {
            println!("data type {:?} not handled", type_byte);
            return None;
        }
    }
}

pub fn encode(data: &RedisType) -> String {
    return match data {
        RedisType::SimpleString(string) => encode_simple_string(string),
        RedisType::SimpleError(error) => encode_simple_error(error),
        RedisType::Integer(integer) => encode_integer(*integer),
        RedisType::BulkString(string) => encode_bulk_string(string.as_ref().map(|x| x.as_str())),
        RedisType::Array(array) => encode_array(array),
        RedisType::Null => encode_null(),
        RedisType::Boolean(boolean) => encode_boolean(*boolean),
        RedisType::Double(double) => encode_double(*double),
        RedisType::BigNumber(big_number) => encode_big_number(big_number),
        RedisType::BulkError(bulk_error) => encode_bulk_error(bulk_error),
        RedisType::VerbatimString(string) => encode_verbatim_string(string),
        RedisType::Push(push) => encode_push(push),
    };
}

pub fn encode_simple_string(string: &str) -> String {
    return format!("+{string}\r\n");
}

pub fn encode_simple_error(error: &str) -> String {
    return format!("-{error}\r\n");
}

pub fn encode_integer(integer: i64) -> String {
    return format!(":{integer}\r\n");
}

pub fn encode_bulk_string(string_option: Option<&str>) -> String {
    if string_option.is_none() {
        return "$-1\r\n".to_owned(); // null bulk string
    }
    let string = string_option.unwrap();
    let length = string.len();
    return format!("${length}\r\n{string}\r\n");
}

pub fn encode_array(array: &Vec<RedisType>) -> String {
    let length = array.len();
    let mut result = format!("*{length}\r\n");
    for item in array {
        result += &encode(&item);
    }
    return result;
}

pub fn encode_null() -> String {
    return "_\r\n".to_string();
}

pub fn encode_boolean(boolean: bool) -> String {
    let boolean_char = if boolean { "t" } else { "f" };
    return format!("#{boolean_char}\r\n");
}

pub fn encode_double(double: f64) -> String {
    return format!(",{double}\r\n");
}

pub fn encode_big_number(big_number: &BigInt) -> String {
    let big_number_string = big_number.to_string();
    return format!("({big_number_string}\r\n");
}

pub fn encode_bulk_error(bulk_error: &str) -> String {
    let length = bulk_error.len();
    return format!("!{length}\r\n{bulk_error}\r\n");
}

pub fn encode_verbatim_string(string: &str) -> String {
    let length = string.len();
    return format!("={length}\r\n{string}\r\n");
}

pub fn encode_push(push: &Vec<RedisType>) -> String {
    let length = push.len();
    let mut result = format!("*{length}\r\n");
    for item in push {
        result += &encode(&item);
    }
    return result;
}

// RDB data is special and shares a signifier byte with bulk strings, so I'm keeping these out of the generic encode/decode methods
pub fn decode_rdb(reader: &mut impl Read) -> Vec<u8> {
    byte_stream::read_byte(reader); // discard type bit
    let length = scan_int(reader).unwrap().0;
    return byte_stream::read_n_bytes(reader, length as usize).unwrap();
}

pub fn encode_rdb(stream: &mut impl Read) -> Vec<u8> {
    let mut rdb_contents = Vec::new();
    let size = stream.read_to_end(&mut rdb_contents).unwrap();
    let mut result_bytes = Vec::from(format!("${size}\r\n").as_bytes());
    result_bytes.append(&mut rdb_contents);
    return result_bytes;
}
