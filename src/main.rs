#[macro_use]
mod handlers;
mod macros;
mod structs;
mod utils;

use std::{
    env,
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
};

use handlers::{client_handler, replication_handler, server_handler};
use structs::server::Server;
use utils::arg_parse;

fn main() {
    let args: Vec<String> = env::args().collect();
    let port = arg_parse::get_u64("--port", &args).unwrap_or(6379);
    let replica_args_option = arg_parse::get_string("--replicaof", &args);
    let dir = arg_parse::get_string("--dir", &args);
    let dbfilename = arg_parse::get_string("--dbfilename", &args);

    let mut master_replid: Option<String> = None;
    let mut master_repl_offset: Option<u64> = None;
    let mut host_stream: Option<TcpStream> = None;
    if replica_args_option.is_some() {
        let replica_args = replica_args_option.as_ref().unwrap();
        let result = client_handler::replicate_server(
            &replica_args.split(' ').map(|s| s.to_owned()).collect(),
            port,
        );
        if result.is_err() {
            println!("{}", result.err().unwrap());
            return;
        }
        let master_info = result.unwrap();
        master_replid = Some(master_info.0);
        master_repl_offset = Some(master_info.1);
        host_stream = Some(master_info.2);
    }

    let server = Arc::new(Server::new(
        replica_args_option,
        master_replid,
        master_repl_offset,
        dir,
        dbfilename,
    ));

    if host_stream.is_some() {
        let server = Arc::clone(&server);
        thread::spawn(move || {
            server_handler::replication_stream_handler(host_stream.unwrap(), server)
        });
    } else {
        let server = Arc::clone(&server);
        thread::spawn(move || {
            replication_handler::replication_loop(server);
        });
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                let server = Arc::clone(&server);
                thread::spawn(move || server_handler::stream_handler(stream, server));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
