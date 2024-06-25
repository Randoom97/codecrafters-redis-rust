use std::io::Write;

use crate::utils::resp_parser::RedisType;

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
