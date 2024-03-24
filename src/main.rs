use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn stream_handler(stream: &mut TcpStream) {
    loop {
        let mut input_buffer = [0; 1024];
        let read_result = stream.read(&mut input_buffer);
        if read_result.is_err() {
            break;
        }

        let write_result = stream.write(b"+PONG\r\n");
        if write_result.is_err() {
            break;
        }
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(mut stream) => {
                stream_handler(&mut stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
