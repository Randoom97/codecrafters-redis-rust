use std::sync::{Mutex, RwLock};

use crate::{
    handlers::replication_handler::Replication, structs::data_store::DataStore,
    structs::xread_subscription::XreadSubscription, utils::rdb,
};

pub struct Server {
    pub role: String,
    pub replid: String,
    pub master_replid: String,
    pub master_repl_offset: RwLock<u64>,
    pub master_repl_mutex: Mutex<()>,
    pub connected_replications: RwLock<Vec<Replication>>,
    pub dir: String,
    pub dbfilename: String,
    pub xread_subscriptions: RwLock<Vec<XreadSubscription>>,
    pub data_store: DataStore,
}

impl Server {
    pub fn new(
        role: Option<Vec<&String>>,
        master_replid: Option<String>,
        master_repl_offset: Option<u64>,
        dir: Option<&String>,
        dbfilename: Option<&String>,
    ) -> Server {
        let server = Server {
            role: (if role.is_none() { "master" } else { "slave" }).to_owned(),
            replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned(), // TODO don't hardcode replid
            master_replid: master_replid
                .unwrap_or("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_owned()),
            master_repl_offset: RwLock::new(master_repl_offset.unwrap_or(0)),
            master_repl_mutex: Mutex::new(()),
            connected_replications: RwLock::new(Vec::new()),
            dir: dir.unwrap_or(&".".to_string()).to_owned(),
            dbfilename: dbfilename.unwrap_or(&"empty.rdb".to_string()).to_owned(),
            xread_subscriptions: RwLock::new(Vec::new()),
            data_store: DataStore::new(),
        };

        let filepath = server.dir.clone() + "/" + &server.dbfilename;
        rdb::load_rdb(&filepath, &server.data_store);
        return server;
    }

    pub fn queue_send_to_replications(&self, command_string: String) {
        let mut master_repl_offset = self.master_repl_offset.write().unwrap();
        *master_repl_offset += command_string.as_bytes().len() as u64;
        drop(master_repl_offset);

        let replication_vec = self.connected_replications.read().unwrap();
        for replication in replication_vec.iter() {
            let mut send_buffer = replication.send_buffer.write().unwrap();
            send_buffer.push(command_string.clone());
        }
    }
}
