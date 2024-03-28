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

fn bulk_string(reader: &mut impl Read) -> Option<Option<String>> {
    option_get_or_return_none!(length, scan_int(reader));
    if length < 0 {
        return Some(None);
    }
    option_get_or_return_none!(bytes, read_n_bytes(reader, length as usize + 2)); // +2 to get the extra crlf
    result_get_or_return_none!(string, from_utf8(&bytes[..length as usize]));
    return Some(Some(string.to_string()));
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
            option_get_or_return_none!(error_option, bulk_string(reader));
            option_get_or_return_none!(error, error_option);
            return Some(RedisType::BulkError(error));
        }
        // verbatim strings
        b'=' => {
            option_get_or_return_none!(string_option, bulk_string(reader));
            option_get_or_return_none!(string, string_option);
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
    read_byte(reader); // discard type bit
    let length = scan_int(reader).unwrap();
    return read_n_bytes(reader, length as usize).unwrap();
}

pub fn encode_rdb(stream: &mut impl Read) -> Vec<u8> {
    let mut rdb_contents = Vec::new();
    let size = stream.read_to_end(&mut rdb_contents).unwrap();
    let mut result_bytes = Vec::from(format!("${size}\r\n").as_bytes());
    result_bytes.append(&mut rdb_contents);
    return result_bytes;
}
