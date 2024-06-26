use std::{
    fs::File,
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, RwLock},
};

use super::{commands, replication_handler::Replication, utils::send};
use crate::{
    macros::option_type_guard,
    utils::resp_parser::{self, RedisType},
    Server,
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

fn psync(mut stream: TcpStream, server: &Arc<Server>) {
    let master_replid = &server.master_replid;
    let master_repl_offset = &server.master_repl_offset.read().unwrap();
    send(
        &mut stream,
        resp_parser::encode_simple_string(&format!(
            "FULLRESYNC {master_replid} {master_repl_offset}"
        )),
    );
    let filepath = server.dir.clone() + "/" + &server.dbfilename;
    let mut empty_rdb_stream = File::open(filepath).unwrap();
    let _ = stream.write(resp_parser::encode_rdb(&mut empty_rdb_stream).as_slice());

    stream.set_nonblocking(true).unwrap();
    let mut stream_vec = server.connected_replications.write().unwrap();
    stream_vec.push(Replication {
        stream,
        send_buffer: RwLock::new(Vec::new()),
        master_repl_offset: RwLock::new(0),
    });
    drop(stream_vec);
}

pub fn replication_stream_handler(mut stream: TcpStream, server: Arc<Server>) {
    loop {
        let arguments_option = parse_arguments(&mut stream);
        if arguments_option.is_none() {
            return; // socket closed or bad parse
        }
        let (arguments, bytes_read) = arguments_option.unwrap();

        let response_option: Option<RedisType> = match arguments[0].to_ascii_lowercase().as_str() {
            "replconf" => Some(commands::replconf(&arguments, &server)), // replconf is the only one that should respond on a replication connection
            "incr" => {
                commands::incr(&arguments, &server, true);
                None
            }
            "set" => {
                commands::set(&arguments, &server, true);
                None
            }
            _ => None,
        };

        if response_option.is_some() {
            send(
                &mut stream,
                resp_parser::encode(response_option.as_ref().unwrap()),
            );
        }

        let mut master_repl_offset = server.master_repl_offset.write().unwrap();
        *master_repl_offset += bytes_read;
        drop(master_repl_offset);
    }
}

pub fn stream_handler(mut stream: TcpStream, server: Arc<Server>) {
    let mut multi_in_process = false;
    let mut multi_queue: Vec<Vec<String>> = Vec::new();
    loop {
        let arguments_option = parse_arguments(&mut stream);
        if arguments_option.is_none() {
            return; // socket closed or bad parse
        }
        let (arguments, _) = arguments_option.unwrap();

        // special commands
        match arguments[0].to_ascii_lowercase().as_str() {
            "exec" => {
                if multi_in_process {
                    let responses = multi_queue
                        .iter()
                        .map(|arguments| handle_command(&arguments, &server))
                        .collect();
                    send(&mut stream, resp_parser::encode_array(&responses));
                    multi_in_process = false;
                    multi_queue.clear();
                } else {
                    send(
                        &mut stream,
                        resp_parser::encode_simple_error("ERR EXEC without MULTI"),
                    )
                }
                continue;
            }
            "multi" => {
                multi_in_process = true;
                send(&mut stream, resp_parser::encode_simple_string("OK"));
                continue;
            }
            "psync" => {
                psync(stream, &server);
                return; // This connection is now a replication connection that will be handled elsewhere
            }
            _ => {}
        }

        if multi_in_process {
            multi_queue.push(arguments);
            send(&mut stream, resp_parser::encode_simple_string("QUEUED"));
            continue;
        }

        send(
            &mut stream,
            resp_parser::encode(&handle_command(&arguments, &server)),
        );
    }
}

fn handle_command(arguments: &Vec<String>, server: &Arc<Server>) -> RedisType {
    return match arguments[0].to_ascii_lowercase().as_str() {
        "incr" => commands::incr(arguments, server, false),
        "xread" => commands::xread(arguments, server),
        "xrange" => commands::xrange(arguments, server),
        "xadd" => commands::xadd(arguments, server),
        "type" => commands::value_type(arguments, server), // can't be 'type' because rust
        "keys" => commands::keys(server),
        "config" => commands::config(arguments, server),
        "wait" => commands::wait(arguments, server),
        "replconf" => commands::replconf(arguments, server),
        "info" => commands::info(server),
        "set" => commands::set(arguments, server, false),
        "get" => commands::get(arguments, server),
        "echo" => RedisType::BulkString(Some(arguments[1].to_owned())),
        "ping" => RedisType::SimpleString("PONG".to_owned()),
        _ => RedisType::SimpleError("Error, unsupported command".to_owned()),
    };
}
