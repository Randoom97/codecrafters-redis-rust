use std::{io::Read, net::TcpStream};

use crate::{
    resp_parser::{self, RedisType},
    utils,
};

fn expect_response(host_stream: &mut impl Read, expected_response: &str) -> bool {
    let (response, _) = resp_parser::decode(host_stream).unwrap();
    option_type_guard!(response_simple_string, response, RedisType::SimpleString);
    return !response_simple_string.is_none()
        && response_simple_string.unwrap().to_ascii_lowercase() == expected_response;
}

pub fn replicate_server(
    replica_args: &Vec<&String>,
    server_port: u64,
) -> Result<(String, u64, TcpStream), String> {
    let master_host = replica_args[0];
    let master_port = replica_args[1];
    let host_stream_result = TcpStream::connect(master_host.to_owned() + ":" + master_port);
    if host_stream_result.is_err() {
        return Err("couldn't connect to master".to_owned());
    }
    let mut host_stream = host_stream_result.unwrap();

    utils::send(
        &mut host_stream,
        resp_parser::encode(&utils::convert_to_redis_command(vec!["ping"])),
    );
    if !expect_response(&mut host_stream, "pong") {
        return Err("master did not respond to ping".to_owned());
    }

    utils::send(
        &mut host_stream,
        resp_parser::encode(&utils::convert_to_redis_command(vec![
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
        resp_parser::encode(&utils::convert_to_redis_command(vec![
            "REPLCONF", "capa", "psync2",
        ])),
    );
    if !expect_response(&mut host_stream, "ok") {
        return Err("master did't respond ok to second REPLCONF".to_owned());
    }

    utils::send(
        &mut host_stream,
        resp_parser::encode(&utils::convert_to_redis_command(vec!["PSYNC", "?", "-1"])),
    );
    let (response, _) = resp_parser::decode(&mut host_stream).unwrap();
    option_type_guard!(response_option, response, RedisType::SimpleString);
    let response = response_option.unwrap();
    let parts = response.split(' ').collect::<Vec<&str>>();
    let master_replid = parts[1];
    let master_repl_offset = str::parse::<u64>(parts[2]).unwrap();

    resp_parser::decode_rdb(&mut host_stream); // do nothing with the rdb for now

    return Ok((master_replid.to_owned(), master_repl_offset, host_stream));
}
