#[macro_use]
mod macros;
mod client;
mod rdb;
mod replication;
mod resp_parser;
mod server;
mod utils;

use std::{
    collections::HashMap,
    env,
    net::{TcpListener, TcpStream},
    sync::{Arc, RwLock},
    thread,
    time::SystemTime,
};

use replication::Replication;
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
    master_repl_offset: RwLock<u64>,
    connected_replications: RwLock<Vec<Replication>>,
    dir: String,
    dbfilename: String,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let port = arg_parse::get_u64("--port", &args).unwrap_or(6379);
    let replica_args_option = arg_parse::get_n_strings("--replicaof", &args, 2);
    let dir = arg_parse::get_string("--dir", &args);
    let dbfilename = arg_parse::get_string("--dbfilename", &args);

    let mut master_replid: Option<String> = None;
    let mut master_repl_offset: Option<u64> = None;
    let mut host_stream: Option<TcpStream> = None;
    if replica_args_option.is_some() {
        let replica_args = replica_args_option.as_ref().unwrap();
        let result = client::replicate_server(replica_args, port);
        if result.is_err() {
            println!("{}", result.err().unwrap());
            return;
        }
        let master_info = result.unwrap();
        master_replid = Some(master_info.0);
        master_repl_offset = Some(master_info.1);
        host_stream = Some(master_info.2);
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
        master_replid: master_replid
            .unwrap_or("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned()),
        master_repl_offset: RwLock::new(master_repl_offset.unwrap_or(0)),
        connected_replications: RwLock::new(Vec::new()),
        dir: dir.unwrap_or(&".".to_string()).to_owned(),
        dbfilename: dbfilename.unwrap_or(&"empty.rdb".to_string()).to_owned(),
    });

    rdb::load_rdb(
        server_info.dir.clone() + "/" + &server_info.dbfilename,
        &data_store,
    );

    if host_stream.is_some() {
        let data_store = Arc::clone(&mut data_store);
        let server_info = Arc::clone(&server_info);
        thread::spawn(move || {
            server::replication_stream_handler(host_stream.unwrap(), data_store, server_info)
        });
    } else {
        let server_info = Arc::clone(&server_info);
        thread::spawn(move || {
            replication::replication_loop(server_info);
        });
    }

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
