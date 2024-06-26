use std::{
    collections::HashSet,
    sync::{mpsc, Arc, MutexGuard},
    thread,
    time::{Duration, SystemTime},
};

use crate::{
    handlers::utils::convert_to_redis_bulk_string_array,
    structs::{
        data_store::DataType,
        xread_subscription::{self, XreadSubscription},
    },
    utils::{arg_parse, resp_parser::RedisType},
    Server,
};

pub fn incr(
    arguments: &Vec<String>,
    server: &Arc<Server>,
    is_replication_connection: bool,
) -> RedisType {
    let key = &arguments[1];

    let mut replication_lock: Option<MutexGuard<()>> = None;
    if !is_replication_connection {
        replication_lock = Some(server.master_repl_mutex.lock().unwrap());
    }

    let result = server.data_store.increment(key);

    // this needs to be inside a lock to guarantee replicas receive commands in the right order
    if !is_replication_connection {
        let arguments_as_str = arguments.iter().map(|s| s.as_str()).collect();
        server.queue_send_to_replications(convert_to_redis_bulk_string_array(arguments_as_str));
    }

    drop(replication_lock);

    if result.is_err() {
        return RedisType::SimpleError("ERR value is not an integer or out of range".to_owned());
    } else {
        return RedisType::Integer(result.unwrap());
    }
}

pub fn xread(arguments: &Vec<String>, server: &Arc<Server>) -> RedisType {
    let block_time = arg_parse::get_u64("block", arguments);
    let streams_index = arguments.iter().position(|a| a == "streams").unwrap();
    let keys_and_ids = &arguments[streams_index + 1..];
    let keys = Vec::from(&keys_and_ids[..keys_and_ids.len() / 2]);
    let mut ids = Vec::from(&keys_and_ids[keys.len()..]);

    server.data_store.xreadids(&keys, &mut ids);

    if block_time.is_some() {
        if block_time.unwrap() == 0 {
            let mut xread_subscriptions = server.xread_subscriptions.write().unwrap();
            let mut wakeup_keys = HashSet::new();
            for key in &keys {
                wakeup_keys.insert(key.clone());
            }
            let (send, recv) = mpsc::channel::<()>();
            xread_subscriptions.push(XreadSubscription::new(wakeup_keys, send));
            drop(xread_subscriptions);

            recv.recv().unwrap(); // block until woken up
        } else {
            thread::sleep(Duration::from_millis(block_time.unwrap()));
        }
    }

    let result = server.data_store.xread(&keys, &ids);

    if result.is_empty() {
        return RedisType::BulkString(None);
    } else {
        return RedisType::Array(result);
    }
}

pub fn xrange(arguments: &Vec<String>, server: &Arc<Server>) -> RedisType {
    let key = &arguments[1];
    let start = &arguments[2];
    let end = &arguments[3];

    return RedisType::Array(server.data_store.xrange(key, start, end));
}

pub fn xadd(arguments: &Vec<String>, server: &Arc<Server>) -> RedisType {
    let key = &arguments[1];
    let id = &arguments[2];
    let fields = &arguments[3..];
    let result = server.data_store.xadd(key, id, fields);

    if result.is_err() {
        return RedisType::SimpleError(result.err().unwrap());
    } else {
        xread_subscription::wakeup_subscribers(server, key);
        return RedisType::BulkString(Some(result.unwrap()));
    }
}

pub fn value_type(arguments: &Vec<String>, server: &Arc<Server>) -> RedisType {
    let key = &arguments[1];

    let data_option = server.data_store.get(key);
    if data_option.is_some() {
        return RedisType::SimpleString(
            match data_option.unwrap() {
                DataType::String(_) => "string",
                DataType::Stream(_) => "stream",
            }
            .to_owned(),
        );
    } else {
        return RedisType::SimpleString("none".to_owned());
    }
}

pub fn keys(server: &Arc<Server>) -> RedisType {
    return convert_to_redis_bulk_string_array(
        server
            .data_store
            .keys()
            .iter()
            .map(|s| s.as_str())
            .collect(),
    );
}

pub fn config(arguments: &Vec<String>, server: &Arc<Server>) -> RedisType {
    return match arguments[2].to_ascii_lowercase().as_str() {
        "dir" => convert_to_redis_bulk_string_array(vec!["dir", &server.dir]),
        "dbfilename" => convert_to_redis_bulk_string_array(vec!["dbfilename", &server.dbfilename]),
        _ => RedisType::SimpleError("Error, unknown config field".to_owned()),
    };
}

pub fn wait(arguments: &Vec<String>, server: &Arc<Server>) -> RedisType {
    let required_replication_count = str::parse::<u64>(&arguments[1]).unwrap();
    let timeout = str::parse::<u64>(&arguments[2]).unwrap();
    let timeout_time = SystemTime::now()
        .checked_add(Duration::from_millis(timeout))
        .unwrap();

    let master_repl_offset = server.master_repl_offset.read().unwrap();
    let expected_offset = *master_repl_offset;
    drop(master_repl_offset);

    // edge case of no commands ever sent
    if expected_offset == 0 {
        let connected_replications = server.connected_replications.read().unwrap();
        let replication_count = connected_replications.len();
        drop(connected_replications);
        return RedisType::Integer(replication_count as i64);
    }

    server.queue_send_to_replications(convert_to_redis_bulk_string_array(vec![
        "REPLCONF", "GETACK", "*",
    ]));

    let mut max_replication_count: u64 = 0;
    loop {
        if SystemTime::now().ge(&timeout_time) {
            break;
        }
        let mut replication_count = 0;
        let connected_replications = server.connected_replications.read().unwrap();
        for replication in connected_replications.iter() {
            let master_repl_offset = replication.master_repl_offset.read().unwrap();
            if *master_repl_offset >= expected_offset {
                replication_count += 1;
            }
        }
        drop(connected_replications);

        if replication_count > max_replication_count {
            max_replication_count = replication_count;
        }

        if max_replication_count >= required_replication_count {
            break;
        }

        thread::sleep(Duration::from_millis(50));
    }

    return RedisType::Integer(max_replication_count as i64);
}

pub fn replconf(arguments: &Vec<String>, server: &Arc<Server>) -> RedisType {
    if arguments[1].to_ascii_lowercase() == "getack" {
        let master_repl_offset = server.master_repl_offset.read().unwrap();
        let master_repl_offset_string = master_repl_offset.to_string();
        drop(master_repl_offset);

        return convert_to_redis_bulk_string_array(vec![
            "REPLCONF",
            "ACK",
            master_repl_offset_string.as_str(),
        ]);
    } else {
        return RedisType::SimpleString("OK".to_owned());
    }
}

pub fn info(server: &Arc<Server>) -> RedisType {
    let role = &server.role;
    let master_replid = &server.replid;
    let master_repl_offset = server.master_repl_offset.read().unwrap();
    let master_repl_offset_clone = master_repl_offset.clone();
    drop(master_repl_offset);

    return RedisType::BulkString(Some(format!(
        "role:{role}\n\
        master_replid:{master_replid}\n\
        master_repl_offset:{master_repl_offset_clone}\n",
    )));
}

pub fn set(
    arguments: &Vec<String>,
    server: &Arc<Server>,
    is_replication_connection: bool,
) -> RedisType {
    let key = &arguments[1];
    let value = &arguments[2];

    let mut expire_time: Option<SystemTime> = None;
    let lifetime_arg = arg_parse::get_u64("px", &arguments);
    if lifetime_arg.is_some() {
        expire_time = Some(
            SystemTime::now()
                .checked_add(Duration::from_millis(lifetime_arg.unwrap()))
                .unwrap(),
        );
    }

    let mut replication_lock: Option<MutexGuard<()>> = None;
    if !is_replication_connection {
        replication_lock = Some(server.master_repl_mutex.lock().unwrap());
    }

    server
        .data_store
        .insert(key, DataType::String(value.to_owned()), expire_time);

    // this needs to be inside a lock to guarantee replicas receive commands in the right order
    if !is_replication_connection {
        let arguments_as_str = arguments.iter().map(|s| s.as_str()).collect();
        server.queue_send_to_replications(convert_to_redis_bulk_string_array(arguments_as_str));
    }

    drop(replication_lock);

    return RedisType::SimpleString("OK".to_owned());
}

pub fn get(arguments: &Vec<String>, server: &Arc<Server>) -> RedisType {
    let key = &arguments[1];

    let data_option = server.data_store.get(key);
    if data_option.is_none() {
        return RedisType::BulkString(None);
    } else {
        return match data_option.unwrap() {
            DataType::String(value) => RedisType::BulkString(Some(value)),
            _ => RedisType::SimpleError("Error, wrongtype operation".to_owned()),
        };
    }
}
