use std::{
    collections::HashMap,
    io::Read,
    net::TcpStream,
    sync::{Arc, RwLock},
};

use crate::{
    resp_parser::{self, RedisType},
    utils, Data, Server,
};

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

mod commands {
    use std::{
        collections::HashMap,
        fs::File,
        io::Write,
        net::TcpStream,
        sync::{Arc, RwLock},
        thread,
        time::{Duration, SystemTime},
    };

    use crate::{
        redis_stream::RedisStream,
        replication::{self, Replication},
        resp_parser,
        utils::{self, arg_parse},
        Data, DataType, Server,
    };

    pub fn xadd(
        stream: &mut impl Write,
        arguments: &Vec<String>,
        data_store: &Arc<RwLock<HashMap<String, Data>>>,
    ) {
        let key = &arguments[1];
        let id = &arguments[2];

        let mut map = data_store.write().unwrap();
        if !map.contains_key(key) {
            map.insert(
                key.clone(),
                Data {
                    value: DataType::Stream(RedisStream::new()),
                    expire_time: None,
                },
            );
        }

        let value = match &mut map.get_mut(key).unwrap().value {
            DataType::Stream(value) => Some(value),
            _ => None,
        }
        .unwrap();

        let mut entry = HashMap::new();
        for i in (3..((arguments.len() / 2) * 2) - 1).step_by(2) {
            entry.insert(arguments[i].clone(), arguments[i + 1].clone());
        }

        let result = value.insert(id.clone(), entry);
        drop(map);

        if result.is_err() {
            utils::send(
                stream,
                resp_parser::encode_simple_error(&result.err().unwrap()),
            )
        } else {
            utils::send(
                stream,
                resp_parser::encode_bulk_string(Some(&result.unwrap())),
            );
        }
    }

    pub fn value_type(
        stream: &mut impl Write,
        arguments: &Vec<String>,
        data_store: &Arc<RwLock<HashMap<String, Data>>>,
    ) {
        let key = &arguments[1];
        let mut response = "none";

        let map = data_store.read().unwrap();
        let data_option = map.get(key);
        if data_option.is_some() {
            let data = data_option.unwrap();
            if data.expire_time.is_none()
                || SystemTime::now().le(data.expire_time.as_ref().unwrap())
            {
                match data.value {
                    DataType::String(_) => response = "string",
                    DataType::Stream(_) => response = "stream",
                }
            }
        }
        drop(map);

        utils::send(stream, resp_parser::encode_simple_string(response));
    }

    pub fn keys(stream: &mut impl Write, data_store: &Arc<RwLock<HashMap<String, Data>>>) {
        let map = data_store.read().unwrap();
        let mut keys: Vec<&String> = map.keys().collect();
        let now = SystemTime::now();
        keys.retain(|key| {
            let value_option = map.get(*key);
            if value_option.is_some() && value_option.unwrap().expire_time.is_some() {
                return value_option.unwrap().expire_time.unwrap().gt(&now);
            }
            return true;
        });

        utils::send(
            stream,
            resp_parser::encode(&utils::convert_to_redis_bulk_string_array(
                keys.iter().map(|s| s.as_str()).collect(),
            )),
        );
    }

    pub fn config(stream: &mut impl Write, arguments: &Vec<String>, server_info: &Arc<Server>) {
        match arguments[2].to_ascii_lowercase().as_str() {
            "dir" => utils::send(
                stream,
                resp_parser::encode(&utils::convert_to_redis_bulk_string_array(vec![
                    "dir",
                    &server_info.dir,
                ])),
            ),
            "dbfilename" => utils::send(
                stream,
                resp_parser::encode(&utils::convert_to_redis_bulk_string_array(vec![
                    "dbfilename",
                    &server_info.dbfilename,
                ])),
            ),
            _ => utils::send(
                stream,
                resp_parser::encode_simple_error("Error, unknown config field"),
            ),
        }
    }

    pub fn wait(stream: &mut impl Write, arguments: &Vec<String>, server_info: &Arc<Server>) {
        let required_replication_count = str::parse::<u64>(&arguments[1]).unwrap();
        let timeout = str::parse::<u64>(&arguments[2]).unwrap();
        let timeout_time = SystemTime::now()
            .checked_add(Duration::from_millis(timeout))
            .unwrap();

        let master_repl_offset = server_info.master_repl_offset.read().unwrap();
        let expected_offset = *master_repl_offset;
        drop(master_repl_offset);

        // edge case of no commands ever sent
        if expected_offset == 0 {
            let connected_replications = server_info.connected_replications.read().unwrap();
            let replication_count = connected_replications.len();
            drop(connected_replications);
            utils::send(
                stream,
                resp_parser::encode_integer(replication_count as i64),
            );
            return;
        }

        replication::queue_send_to_replications(
            &server_info,
            resp_parser::encode(&utils::convert_to_redis_bulk_string_array(vec![
                "REPLCONF", "GETACK", "*",
            ])),
        );

        let mut max_replication_count: u64 = 0;
        loop {
            if SystemTime::now().ge(&timeout_time) {
                break;
            }
            let mut replication_count = 0;
            let connected_replications = server_info.connected_replications.read().unwrap();
            for replication in connected_replications.iter() {
                let master_repl_offset = replication.master_repl_offset.read().unwrap();
                if *master_repl_offset >= expected_offset {
                    replication_count += 1;
                }
            }
            drop(connected_replications);

            if replication_count > max_replication_count {
                max_replication_count = replication_count;
            }

            if max_replication_count >= required_replication_count {
                break;
            }

            thread::sleep(Duration::from_millis(50));
        }

        utils::send(
            stream,
            resp_parser::encode_integer(max_replication_count as i64),
        );
    }

    pub fn psync(mut stream: TcpStream, server_info: &Arc<Server>) {
        let master_replid = &server_info.master_replid;
        let master_repl_offset = &server_info.master_repl_offset.read().unwrap();
        utils::send(
            &mut stream,
            resp_parser::encode_simple_string(&format!(
                "FULLRESYNC {master_replid} {master_repl_offset}"
            )),
        );
        let filepath = server_info.dir.clone() + "/" + &server_info.dbfilename;
        let mut empty_rdb_stream = File::open(filepath).unwrap();
        let _ = stream.write(resp_parser::encode_rdb(&mut empty_rdb_stream).as_slice());

        stream.set_nonblocking(true).unwrap();
        let mut stream_vec = server_info.connected_replications.write().unwrap();
        stream_vec.push(Replication {
            stream,
            send_buffer: RwLock::new(Vec::new()),
            master_repl_offset: RwLock::new(0),
        });
        drop(stream_vec);
    }

    pub fn replconf(stream: &mut impl Write, arguments: &Vec<String>, server_info: &Arc<Server>) {
        if arguments[1].to_ascii_lowercase() == "getack" {
            let master_repl_offset = server_info.master_repl_offset.read().unwrap();
            let master_repl_offset_string = master_repl_offset.to_string();
            drop(master_repl_offset);

            utils::send(
                stream,
                resp_parser::encode(&utils::convert_to_redis_bulk_string_array(vec![
                    "REPLCONF",
                    "ACK",
                    master_repl_offset_string.as_str(),
                ])),
            )
        } else {
            utils::send(stream, resp_parser::encode_simple_string("OK"));
        }
    }

    pub fn info(stream: &mut impl Write, server_info: &Arc<Server>) {
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

    pub fn set(
        stream: &mut impl Write,
        arguments: &Vec<String>,
        data_store: &Arc<RwLock<HashMap<String, Data>>>,
        server_info: &Arc<Server>,
        is_replication_connection: bool,
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
                value: DataType::String(value.to_owned()),
                expire_time,
            },
        );
        // this needs to be inside the map lock to guarantee replicas receive commands in the right order
        if !is_replication_connection {
            let arguments_as_str = arguments.iter().map(|s| s.as_str()).collect();
            let command =
                resp_parser::encode(&utils::convert_to_redis_bulk_string_array(arguments_as_str));
            replication::queue_send_to_replications(&server_info, command);
        }
        drop(map);

        if !is_replication_connection {
            utils::send(stream, resp_parser::encode_simple_string("OK"));
        }
    }

    pub fn get(
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
            if data.expire_time.is_some()
                && SystemTime::now().gt(data.expire_time.as_ref().unwrap())
            {
                expired = true;
            } else {
                match &data.value {
                    DataType::String(value) => {
                        response = Some(resp_parser::encode_bulk_string(Some(value)))
                    }
                    _ => {
                        response = Some(resp_parser::encode_simple_error(
                            "Error, wrongtype operation",
                        ))
                    }
                }
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
            "replconf" => commands::replconf(&mut stream, &arguments, &server_info),
            "set" => commands::set(&mut stream, &arguments, &data_store, &server_info, true),
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
            "xadd" => commands::xadd(&mut stream, &arguments, &data_store),
            "type" => commands::value_type(&mut stream, &arguments, &data_store), // can't be 'type' because rust
            "keys" => commands::keys(&mut stream, &data_store),
            "config" => commands::config(&mut stream, &arguments, &server_info),
            "wait" => commands::wait(&mut stream, &arguments, &server_info),
            "psync" => {
                commands::psync(stream, &server_info);
                return; // This connection is now a replication connection that will be handled elsewhere
            }
            "replconf" => commands::replconf(&mut stream, &arguments, &server_info),
            "info" => commands::info(&mut stream, &server_info),
            "set" => commands::set(&mut stream, &arguments, &data_store, &server_info, false),
            "get" => commands::get(&mut stream, &arguments, &data_store),
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
