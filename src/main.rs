#[macro_use]
mod macros;
mod parser;

use std::{
    collections::HashMap,
    env,
    io::Write,
    net::{TcpListener, TcpStream},
    sync::{Arc, RwLock},
    thread,
    time::{Duration, SystemTime},
};

use parser::{decode, encode_bulk_string, encode_simple_error, encode_simple_string, RedisType};

#[derive(Debug)]
struct Data {
    value: String,
    expire_time: Option<SystemTime>,
}

struct Server {
    role: String,
}

fn send(stream: &mut impl Write, message: String) {
    stream.write(message.as_bytes()).unwrap();
}

fn get_n_arguments<'a>(token: &str, arguments: &'a Vec<String>, n: u64) -> Option<Vec<&'a String>> {
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

fn get_argument<'a>(token: &str, arguments: &'a Vec<String>) -> Option<&'a String> {
    option_get_or_return_none!(argument_vec, get_n_arguments(token, arguments, 1));
    return Some(argument_vec[0]);
}

fn get_u64_argument(token: &str, arguments: &Vec<String>) -> Option<u64> {
    let string_option = get_argument(token, arguments);
    if string_option.is_none() {
        return None;
    }
    return str::parse::<u64>(string_option.unwrap()).ok();
}

fn stream_handler(
    mut stream: TcpStream,
    data_store: Arc<RwLock<HashMap<String, Data>>>,
    server_info: Arc<Server>,
) {
    loop {
        let input_option = decode(&mut stream);
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
            "info" => send(
                &mut stream,
                encode_bulk_string(Some(&("role:".to_owned() + &server_info.role))),
            ),
            "set" => {
                let key = &arguments[1];
                let value = &arguments[2];

                let mut expire_time: Option<SystemTime> = None;
                let lifetime_arg = get_u64_argument("px", &arguments);
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

                send(&mut stream, encode_simple_string("OK"));
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
                        response = Some(encode_bulk_string(Some(&data.value)));
                    }
                }
                drop(map);

                if response.is_none() {
                    send(&mut stream, encode_bulk_string(None));
                } else {
                    send(&mut stream, response.unwrap());
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
                send(&mut stream, encode_bulk_string(Some(&arguments[1])));
            }
            "ping" => {
                send(&mut stream, encode_simple_string("PONG"));
            }
            _ => {
                send(
                    &mut stream,
                    encode_simple_error("Error, unsupported command"),
                );
            }
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let port = get_u64_argument("--port", &args);
    let replica_args = get_n_arguments("--replicaof", &args, 2);

    let mut data_store: Arc<RwLock<HashMap<String, Data>>> = Arc::new(RwLock::new(HashMap::new()));
    let server_info = Arc::new(Server {
        role: (if replica_args.is_none() {
            "master"
        } else {
            "slave"
        })
        .to_owned(),
    });

    let listener =
        TcpListener::bind("127.0.0.1:".to_owned() + &port.unwrap_or(6379).to_string()).unwrap();

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                let data_store = Arc::clone(&mut data_store);
                let server_info = Arc::clone(&server_info);
                thread::spawn(move || stream_handler(stream, data_store, server_info));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
