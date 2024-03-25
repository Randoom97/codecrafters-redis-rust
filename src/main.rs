#[macro_use]
mod macros;
mod parser;

use std::{
    io::Write,
    net::{TcpListener, TcpStream},
    thread,
};

use parser::{decode, encode_simple_error, encode_simple_string, RedisType};

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
        let arguments = arguments_option.unwrap();
        option_type_guard!(command_option, &arguments[0], RedisType::BulkString);
        match command_option.unwrap().to_ascii_lowercase().as_str() {
            "echo" => {
                option_type_guard!(arg1_option, &arguments[1], RedisType::BulkString);
                send(&mut stream, encode_simple_string(arg1_option.unwrap()));
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
