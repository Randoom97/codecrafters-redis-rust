use std::{io::Read, net::TcpStream};

use crate::{
    resp_parser::{self, RedisType},
    utils,
};

/// converts a Vec\<String\> to RedisType::Array\<RedisType::BulkString\>
fn convert_to_redis_command(command: Vec<&str>) -> RedisType {
    let mut bulk_string_command: Vec<RedisType> = Vec::new();
    for part in command {
        bulk_string_command.push(RedisType::BulkString(Some(part.to_owned())));
    }
    return RedisType::Array(bulk_string_command);
}

fn expect_response(host_stream: &mut impl Read, expected_response: &str) -> bool {
    option_type_guard!(
        response_simple_string,
        resp_parser::decode(host_stream).unwrap(),
        RedisType::SimpleString
    );
    return !response_simple_string.is_none()
        && response_simple_string.unwrap().to_ascii_lowercase() == expected_response;
}

pub fn replicate_server(replica_args: &Vec<&String>, server_port: u64) -> Result<(), String> {
    let master_host = replica_args[0];
    let master_port = replica_args[1];
    let host_stream_result = TcpStream::connect(master_host.to_owned() + ":" + master_port);
    if host_stream_result.is_err() {
        return Err("couldn't connect to master".to_owned());
    }
    let mut host_stream = host_stream_result.unwrap();

    utils::send(
        &mut host_stream,
        resp_parser::encode(&convert_to_redis_command(vec!["ping"])),
    );
    if !expect_response(&mut host_stream, "pong") {
        return Err("master did not respond to ping".to_owned());
    }

    utils::send(
        &mut host_stream,
        resp_parser::encode(&convert_to_redis_command(vec![
            "REPLCONF",
            "listening-port",
            &server_port.to_string(),
        ])),
    );
    if !expect_response(&mut host_stream, "ok") {
        return Err("master didn't respond ok to first REPLCONF".to_owned());
    }

    utils::send(
        &mut host_stream,
        resp_parser::encode(&convert_to_redis_command(vec![
            "REPLCONF", "capa", "psync2",
        ])),
    );
    if !expect_response(&mut host_stream, "ok") {
        return Err("master did't respond ok to second REPLCONF".to_owned());
    }

    return Ok(());
}
