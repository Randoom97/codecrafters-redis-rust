use std::{
    collections::HashMap,
    net::TcpStream,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use crate::{
    resp_parser::{self, RedisType},
    utils::{self, arg_parse},
    Data, Server,
};

pub fn stream_handler(
    mut stream: TcpStream,
    data_store: Arc<RwLock<HashMap<String, Data>>>,
    server_info: Arc<Server>,
) {
    loop {
        let input_option = resp_parser::decode(&mut stream);
        if input_option.is_none() {
            break; // socket closed or bad parse
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
            "replconf" => {
                utils::send(&mut stream, resp_parser::encode_simple_string("OK"));
            }
            "info" => {
                let role = &server_info.role;
                let master_replid = &server_info.master_replid;
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
                drop(map);

                utils::send(&mut stream, resp_parser::encode_simple_string("OK"));
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
