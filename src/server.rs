use std::{
    collections::HashMap,
    fs::File,
    io::Write,
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

pub fn stream_handler(
    mut stream: TcpStream,
    data_store: Arc<RwLock<HashMap<String, Data>>>,
    server_info: Arc<Server>,
    should_send_on_write: bool,
) {
    loop {
        let input_option = resp_parser::decode(&mut stream);
        if input_option.is_none() {
            return; // socket closed or bad parse
        }
        option_type_guard!(arguments_option, input_option.unwrap(), RedisType::Array);
        // clients should only be sending arrays of bulk strings
        let arguments: Vec<String> = arguments_option
            .unwrap()
            .iter()
            .map(|v| {
                option_type_guard!(string, v, RedisType::BulkString);
                return string.unwrap().as_ref().unwrap().to_owned();
            })
            .collect();

        match arguments[0].to_ascii_lowercase().as_str() {
            "psync" => {
                let master_replid = &server_info.master_replid;
                let master_repl_offset = &server_info.master_repl_offset;
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

                return; // This connection is now a replication connection that will be handled elsewhere
            }
            "replconf" => {
                utils::send(&mut stream, resp_parser::encode_simple_string("OK"));
            }
            "info" => {
                let role = &server_info.role;
                let master_replid = &server_info.replid;
                let master_repl_offset = &server_info.master_repl_offset;
                utils::send(
                    &mut stream,
                    resp_parser::encode_bulk_string(Some(&format!(
                        "role:{role}\n\
                        master_replid:{master_replid}\n\
                        master_repl_offset:{master_repl_offset}\n",
                    ))),
                )
            }
            "set" => {
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

                if should_send_on_write {
                    utils::send(&mut stream, resp_parser::encode_simple_string("OK"));
                }
            }
            "get" => {
                let key = &arguments[1];
                let mut response: Option<String> = None;
                let mut expired = false;

                let map = data_store.read().unwrap();
                let data_option = map.get(key);
                if data_option.is_some() {
                    let data = data_option.unwrap();
                    if data.expire_time.is_some()
                        && SystemTime::now().gt(data.expire_time.as_ref().unwrap())
                    {
                        expired = true;
                        response = None;
                    } else {
                        response = Some(resp_parser::encode_bulk_string(Some(&data.value)));
                    }
                }
                drop(map);

                if response.is_none() {
                    utils::send(&mut stream, resp_parser::encode_bulk_string(None));
                } else {
                    utils::send(&mut stream, response.unwrap());
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
            "echo" => {
                utils::send(
                    &mut stream,
                    resp_parser::encode_bulk_string(Some(&arguments[1])),
                );
            }
            "ping" => {
                utils::send(&mut stream, resp_parser::encode_simple_string("PONG"));
            }
            _ => {
                utils::send(
                    &mut stream,
                    resp_parser::encode_simple_error("Error, unsupported command"),
                );
            }
        }
    }
}
