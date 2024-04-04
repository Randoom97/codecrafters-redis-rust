use std::io::Write;

use crate::resp_parser::RedisType;

pub fn send(stream: &mut impl Write, message: String) {
    stream.write(message.as_bytes()).unwrap();
}

/// converts a Vec\<String\> to RedisType::Array\<RedisType::BulkString\>
pub fn convert_to_redis_bulk_string_array(strings: Vec<&str>) -> RedisType {
    let mut bulk_string_command: Vec<RedisType> = Vec::new();
    for part in strings {
        bulk_string_command.push(RedisType::BulkString(Some(part.to_owned())));
    }
    return RedisType::Array(bulk_string_command);
}

pub mod byte_stream {
    use std::io::Read;

    pub fn read_n_bytes(reader: &mut impl Read, n: usize) -> Option<Vec<u8>> {
        let mut buffer = vec![0u8; n];
        result_get_or_return_none!(_unused, reader.read_exact(&mut buffer));
        return Some(buffer);
    }

    pub fn read_byte(reader: &mut impl Read) -> Option<u8> {
        option_get_or_return_none!(byte_vec, read_n_bytes(reader, 1));
        return Some(byte_vec[0]);
    }
}

pub mod arg_parse {
    pub fn get_n_strings<'a>(
        token: &str,
        arguments: &'a Vec<String>,
        n: u64,
    ) -> Option<Vec<&'a String>> {
        let position = arguments
            .iter()
            .position(|s| s.to_ascii_lowercase() == token);
        if position.is_none() {
            return None;
        }
        let mut result: Vec<&String> = Vec::new();
        for i in 0..n {
            let argument = arguments.get(position.unwrap() + 1 + i as usize);
            if argument.is_none() {
                return None;
            }
            result.push(argument.unwrap());
        }
        return Some(result);
    }

    pub fn get_string<'a>(token: &str, arguments: &'a Vec<String>) -> Option<&'a String> {
        option_get_or_return_none!(argument_vec, get_n_strings(token, arguments, 1));
        return Some(argument_vec[0]);
    }

    pub fn get_u64(token: &str, arguments: &Vec<String>) -> Option<u64> {
        let string_option = get_string(token, arguments);
        if string_option.is_none() {
            return None;
        }
        return str::parse::<u64>(string_option.unwrap()).ok();
    }
}
