use std::{
    io::Write,
    net::TcpStream,
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

use crate::{
    macros::option_type_guard,
    utils::resp_parser::{self, RedisType},
    Server,
};

pub struct Replication {
    pub(crate) stream: TcpStream, // this should be set to non blocking mode before constructing
    pub(crate) send_buffer: RwLock<Vec<String>>,
    pub(crate) master_repl_offset: RwLock<u64>,
}

pub fn replication_loop(server: Arc<Server>) {
    loop {
        let mut replication_vec = server.connected_replications.write().unwrap();
        let mut drop_indicies: Vec<usize> = Vec::new();
        for i in 0..replication_vec.len() {
            let replication = &mut replication_vec[i];

            // non blocking read for getack response for offset
            let response_and_count_option = resp_parser::decode(&mut replication.stream);
            if response_and_count_option.is_some() {
                let (response, _) = response_and_count_option.unwrap();
                option_type_guard!(response_array, response, RedisType::Array);
                let response_offset_part = &response_array.unwrap()[2];
                option_type_guard!(offset_string, response_offset_part, RedisType::BulkString);

                let offset = str::parse::<u64>(offset_string.unwrap().as_ref().unwrap()).unwrap();
                let mut master_repl_offset = replication.master_repl_offset.write().unwrap();
                *master_repl_offset = offset;
            }

            // check for data to send
            let mut send_buffer = replication.send_buffer.write().unwrap();
            if send_buffer.is_empty() {
                continue;
            }
            let command = send_buffer.remove(0);
            let write_result = replication.stream.write(&command.as_bytes());

            if write_result.is_err() {
                drop_indicies.push(i as usize);
            }
        }

        // remove the replications that failed on write (most likely due to the socket closing)
        drop_indicies.reverse();
        for index in drop_indicies {
            replication_vec.remove(index);
        }
        drop(replication_vec);

        thread::sleep(Duration::from_millis(50));
    }
}
