use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use crate::{
    resp_parser::{self, RedisType},
    utils::{self, arg_parse},
    Data, Server,
};

fn send_to_replications(server_info: &Arc<Server>, arguments: &Vec<String>) {
    let mut stream_vec = server_info.connected_replications.write().unwrap();
    let arguments_as_str = arguments.iter().map(|s| s.as_str()).collect();
    let command = resp_parser::encode(&utils::convert_to_redis_command(arguments_as_str));
    // retain to remove any connections that closed
    stream_vec.retain(|mut stream| {
        let write_result = stream.write(&command.as_bytes());
        return !write_result.is_err();
    });
}

fn parse_arguments(stream: &mut impl Read) -> Option<(Vec<String>, u64)> {
    let input_option = resp_parser::decode(stream);
    if input_option.is_none() {
        return None; // socket closed or bad parse
    }
    let (input, bytes_read) = input_option.unwrap();
    option_type_guard!(arguments_option, input, RedisType::Array);
    // clients should only be sending arrays of bulk strings
    return Some((
        arguments_option
            .unwrap()
            .iter()
            .map(|v| {
                option_type_guard!(string, v, RedisType::BulkString);
                return string.unwrap().as_ref().unwrap().to_owned();
            })
            .collect(),
        bytes_read,
    ));
}

fn wait(stream: &mut impl Write, arguments: &Vec<String>, server_info: &Arc<Server>) {
    let connected_replications = server_info.connected_replications.read().unwrap();
    let replication_count = connected_replications.len();
    drop(connected_replications);
    utils::send(
        stream,
        resp_parser::encode_integer(replication_count as i64),
    );
}

fn psync(mut stream: TcpStream, server_info: &Arc<Server>) {
    let master_replid = &server_info.master_replid;
    let master_repl_offset = &server_info.master_repl_offset.read().unwrap();
    utils::send(
        &mut stream,
        resp_parser::encode_simple_string(&format!(
            "FULLRESYNC {master_replid} {master_repl_offset}"
        )),
    );
    let mut empty_rdb_stream = File::open("empty.rdb").unwrap();
    let _ = stream.write(resp_parser::encode_rdb(&mut empty_rdb_stream).as_slice());

    let mut stream_vec = server_info.connected_replications.write().unwrap();
    stream_vec.push(stream);
    drop(stream_vec);
}

fn replconf(stream: &mut impl Write, arguments: &Vec<String>, server_info: &Arc<Server>) {
    if arguments[1].to_ascii_lowercase() == "getack" {
        let master_repl_offset = server_info.master_repl_offset.read().unwrap();
        let master_repl_offset_string = master_repl_offset.to_string();
        drop(master_repl_offset);

        utils::send(
            stream,
            resp_parser::encode(&utils::convert_to_redis_command(vec![
                "REPLCONF",
                "ACK",
                master_repl_offset_string.as_str(),
            ])),
        )
    } else {
        utils::send(stream, resp_parser::encode_simple_string("OK"));
    }
}

fn info(stream: &mut impl Write, server_info: &Arc<Server>) {
    let role = &server_info.role;
    let master_replid = &server_info.replid;
    let master_repl_offset = server_info.master_repl_offset.read().unwrap();
    let master_repl_offset_clone = master_repl_offset.clone();
    drop(master_repl_offset);

    utils::send(
        stream,
        resp_parser::encode_bulk_string(Some(&format!(
            "role:{role}\n\
            master_replid:{master_replid}\n\
            master_repl_offset:{master_repl_offset_clone}\n",
        ))),
    )
}

fn set(
    stream: &mut impl Write,
    arguments: &Vec<String>,
    data_store: &Arc<RwLock<HashMap<String, Data>>>,
    server_info: &Arc<Server>,
    should_send_response: bool,
) {
    let key = &arguments[1];
    let value = &arguments[2];

    let mut expire_time: Option<SystemTime> = None;
    let lifetime_arg = arg_parse::get_u64("px", &arguments);
    if lifetime_arg.is_some() {
        expire_time = Some(
            SystemTime::now()
                .checked_add(Duration::from_millis(lifetime_arg.unwrap()))
                .unwrap(),
        );
    }

    let mut map = data_store.write().unwrap();
    map.insert(
        key.to_owned(),
        Data {
            value: value.to_owned(),
            expire_time,
        },
    );
    // this needs to be inside the map lock to guarantee replicas receive commands in the right order
    send_to_replications(&server_info, &arguments);
    drop(map);

    if should_send_response {
        utils::send(stream, resp_parser::encode_simple_string("OK"));
    }
}

fn get(
    stream: &mut impl Write,
    arguments: &Vec<String>,
    data_store: &Arc<RwLock<HashMap<String, Data>>>,
) {
    let key = &arguments[1];
    let mut response: Option<String> = None;
    let mut expired = false;

    let map = data_store.read().unwrap();
    let data_option = map.get(key);
    if data_option.is_some() {
        let data = data_option.unwrap();
        if data.expire_time.is_some() && SystemTime::now().gt(data.expire_time.as_ref().unwrap()) {
            expired = true;
            response = None;
        } else {
            response = Some(resp_parser::encode_bulk_string(Some(&data.value)));
        }
    }
    drop(map);

    if response.is_none() {
        utils::send(stream, resp_parser::encode_bulk_string(None));
    } else {
        utils::send(stream, response.unwrap());
    }

    // cleanup if we found the value expired
    if expired {
        // make sure it didn't get updated in the short time the lock was released before removing
        let mut map = data_store.write().unwrap();
        let data_option = map.get(key);
        if data_option.is_some()
            && data_option.unwrap().expire_time.is_some()
            && SystemTime::now().gt(data_option.unwrap().expire_time.as_ref().unwrap())
        {
            map.remove(key);
        }
        drop(map);
    }
}

pub fn replication_stream_handler(
    mut stream: TcpStream,
    data_store: Arc<RwLock<HashMap<String, Data>>>,
    server_info: Arc<Server>,
) {
    loop {
        let arguments_option = parse_arguments(&mut stream);
        if arguments_option.is_none() {
            return; // socket closed or bad parse
        }
        let (arguments, bytes_read) = arguments_option.unwrap();

        match arguments[0].to_ascii_lowercase().as_str() {
            "replconf" => replconf(&mut stream, &arguments, &server_info),
            "set" => set(&mut stream, &arguments, &data_store, &server_info, false),
            _ => {}
        }

        let mut master_repl_offset = server_info.master_repl_offset.write().unwrap();
        *master_repl_offset += bytes_read;
        drop(master_repl_offset);
    }
}

pub fn stream_handler(
    mut stream: TcpStream,
    data_store: Arc<RwLock<HashMap<String, Data>>>,
    server_info: Arc<Server>,
) {
    loop {
        let arguments_option = parse_arguments(&mut stream);
        if arguments_option.is_none() {
            return; // socket closed or bad parse
        }
        let (arguments, _) = arguments_option.unwrap();

        match arguments[0].to_ascii_lowercase().as_str() {
            "wait" => wait(&mut stream, &arguments, &server_info),
            "psync" => {
                psync(stream, &server_info);
                return; // This connection is now a replication connection that will be handled elsewhere
            }
            "replconf" => replconf(&mut stream, &arguments, &server_info),
            "info" => info(&mut stream, &server_info),
            "set" => set(&mut stream, &arguments, &data_store, &server_info, true),
            "get" => get(&mut stream, &arguments, &data_store),
            "echo" => utils::send(
                &mut stream,
                resp_parser::encode_bulk_string(Some(&arguments[1])),
            ),
            "ping" => utils::send(&mut stream, resp_parser::encode_simple_string("PONG")),
            _ => {
                utils::send(
                    &mut stream,
                    resp_parser::encode_simple_error("Error, unsupported command"),
                );
            }
        }
    }
}
