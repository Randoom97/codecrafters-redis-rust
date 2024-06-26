use std::{io::Read, net::TcpStream, sync::Arc};

use super::{commands, utils::send};
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

pub fn replication_stream_handler(mut stream: TcpStream, server: Arc<Server>) {
    loop {
        let arguments_option = parse_arguments(&mut stream);
        if arguments_option.is_none() {
            return; // socket closed or bad parse
        }
        let (arguments, bytes_read) = arguments_option.unwrap();

        match arguments[0].to_ascii_lowercase().as_str() {
            "incr" => commands::incr(&mut stream, &arguments, &server, true),
            "replconf" => commands::replconf(&mut stream, &arguments, &server),
            "set" => commands::set(&mut stream, &arguments, &server, true),
            _ => {}
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

        if multi_in_process {
            multi_queue.push(arguments);
            send(&mut stream, resp_parser::encode_simple_string("QUEUED"));
            continue;
        }

        match arguments[0].to_ascii_lowercase().as_str() {
            "multi" => {
                multi_in_process = true;
                send(&mut stream, resp_parser::encode_simple_string("OK"));
            }
            "incr" => commands::incr(&mut stream, &arguments, &server, false),
            "xread" => commands::xread(&mut stream, &arguments, &server),
            "xrange" => commands::xrange(&mut stream, &arguments, &server),
            "xadd" => commands::xadd(&mut stream, &arguments, &server),
            "type" => commands::value_type(&mut stream, &arguments, &server), // can't be 'type' because rust
            "keys" => commands::keys(&mut stream, &server),
            "config" => commands::config(&mut stream, &arguments, &server),
            "wait" => commands::wait(&mut stream, &arguments, &server),
            "psync" => {
                commands::psync(stream, &server);
                return; // This connection is now a replication connection that will be handled elsewhere
            }
            "replconf" => commands::replconf(&mut stream, &arguments, &server),
            "info" => commands::info(&mut stream, &server),
            "set" => commands::set(&mut stream, &arguments, &server, false),
            "get" => commands::get(&mut stream, &arguments, &server),
            "echo" => send(
                &mut stream,
                resp_parser::encode_bulk_string(Some(&arguments[1])),
            ),
            "ping" => send(&mut stream, resp_parser::encode_simple_string("PONG")),
            _ => {
                send(
                    &mut stream,
                    resp_parser::encode_simple_error("Error, unsupported command"),
                );
            }
        }
    }
}
