#[macro_use]
mod macros;
mod parser;

use std::{
    collections::HashMap,
    io::Write,
    net::{TcpListener, TcpStream},
    sync::RwLock,
    thread,
};

use parser::{
    decode, encode, encode_bulk_string, encode_simple_error, encode_simple_string, RedisType,
};

static mut DATA_STORE: Option<RwLock<HashMap<String, RedisType>>> = None;

fn send(stream: &mut impl Write, message: String) {
    stream.write(message.as_bytes()).unwrap();
}

fn stream_handler(mut stream: TcpStream) {
    loop {
        let input_option = decode(&mut stream);
        if input_option.is_none() {
            continue;
        }
        option_type_guard!(arguments_option, input_option.unwrap(), RedisType::Array);
        let mut arguments = arguments_option.unwrap();
        option_type_guard!(command_option, &arguments[0], RedisType::BulkString);
        match command_option
            .unwrap()
            .as_ref()
            .unwrap()
            .to_ascii_lowercase()
            .as_str()
        {
            "set" => {
                option_type_guard!(key_option, &arguments[1], RedisType::BulkString);
                let key = key_option.unwrap().to_owned().unwrap();
                unsafe {
                    let mut map = DATA_STORE.as_ref().unwrap().write().unwrap();
                    map.insert(key, arguments.swap_remove(2));
                }
                send(&mut stream, encode_simple_string("OK"));
            }
            "get" => {
                option_type_guard!(key_option, &arguments[1], RedisType::BulkString);
                let key = key_option.unwrap().as_ref().unwrap();
                let mut response: Option<String> = None;
                unsafe {
                    let map = DATA_STORE.as_ref().unwrap().read().unwrap();
                    let value = map.get(key);
                    if value.is_some() {
                        response = Some(encode(value.unwrap()));
                    }
                }
                if response.is_none() {
                    send(&mut stream, encode_bulk_string(None));
                } else {
                    send(&mut stream, response.unwrap());
                }
            }
            "echo" => {
                send(&mut stream, encode(&arguments[1]));
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
    unsafe {
        DATA_STORE = Some(RwLock::new(HashMap::new()));
    }
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                thread::spawn(|| stream_handler(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
