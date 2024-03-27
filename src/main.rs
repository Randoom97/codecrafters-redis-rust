#[macro_use]
mod macros;
mod client;
mod resp_parser;
mod server;
mod utils;

use std::{
    collections::HashMap,
    env,
    net::TcpListener,
    sync::{Arc, RwLock},
    thread,
    time::SystemTime,
};

use utils::arg_parse;

#[derive(Debug)]
struct Data {
    value: String,
    expire_time: Option<SystemTime>,
}

struct Server {
    role: String,
    replid: String,
    master_replid: String,
    master_repl_offset: u64,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let port = arg_parse::get_u64("--port", &args).unwrap_or(6379);
    let replica_args_option = arg_parse::get_n_strings("--replicaof", &args, 2);

    if replica_args_option.is_some() {
        let replica_args = replica_args_option.as_ref().unwrap();
        let result = client::replicate_server(replica_args, port);
        if result.is_err() {
            println!("{}", result.err().unwrap());
            return;
        }
    }

    let mut data_store: Arc<RwLock<HashMap<String, Data>>> = Arc::new(RwLock::new(HashMap::new()));
    let server_info = Arc::new(Server {
        role: (if replica_args_option.is_none() {
            "master"
        } else {
            "slave"
        })
        .to_owned(),
        replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned(), // TODO don't hardcode replid
        master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned(),
        master_repl_offset: 0,
    });

    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();

    for stream_result in listener.incoming() {
        match stream_result {
            Ok(stream) => {
                let data_store = Arc::clone(&mut data_store);
                let server_info = Arc::clone(&server_info);
                thread::spawn(move || server::stream_handler(stream, data_store, server_info));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
