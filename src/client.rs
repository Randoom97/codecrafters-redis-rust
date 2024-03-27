use std::net::TcpStream;

use crate::{
    resp_parser::{self, RedisType},
    utils,
};

pub fn replicate_server(replica_args: &Vec<&String>) -> Result<(), String> {
    let master_host = replica_args[0];
    let master_port = replica_args[1];
    let host_stream_result = TcpStream::connect(master_host.to_owned() + ":" + master_port);
    if host_stream_result.is_err() {
        return Err("couldn't connect to master".to_owned());
    }
    let mut host_stream = host_stream_result.unwrap();
    utils::send(
        &mut host_stream,
        resp_parser::encode(&RedisType::Array(vec![RedisType::BulkString(Some(
            "ping".to_owned(),
        ))])),
    );
    option_type_guard!(
        response_bulk_string,
        resp_parser::decode(&mut host_stream).unwrap(),
        RedisType::SimpleString
    );
    if response_bulk_string.is_none()
        || response_bulk_string.unwrap().to_ascii_lowercase() != "pong".to_owned()
    {
        return Err("master did not respond to ping".to_owned());
    }
    return Ok(());
}
