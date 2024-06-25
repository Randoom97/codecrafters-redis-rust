use std::{collections::HashMap, sync::RwLock, time::SystemTime};

use crate::{structs::redis_stream::RedisStream, utils::resp_parser::RedisType};

#[derive(Debug, Clone)]
pub enum DataType {
    String(String),
    Stream(RedisStream),
}

struct DataMaps {
    data: HashMap<String, DataType>,
    expire_times: HashMap<String, SystemTime>,
}

pub struct DataStore {
    maps: RwLock<DataMaps>,
}

impl DataStore {
    pub fn new() -> DataStore {
        return DataStore {
            maps: RwLock::new(DataMaps {
                data: HashMap::new(),
                expire_times: HashMap::new(),
            }),
        };
    }

    pub fn get(&self, key: &String) -> Option<DataType> {
        let expired: bool;

        let maps = self.maps.read().unwrap();
        if maps.expire_times.contains_key(key)
            && SystemTime::now().gt(maps.expire_times.get(key).unwrap())
        {
            expired = true;
        } else {
            return maps.data.get(key).cloned();
        }
        drop(maps);

        // cleanup if we found the value expired
        if expired {
            // make sure it didn't get updated in the short time the lock was released before removing
            let mut maps = self.maps.write().unwrap();
            if maps.expire_times.contains_key(key)
                && SystemTime::now().gt(maps.expire_times.get(key).unwrap())
            {
                maps.expire_times.remove(key);
                maps.data.remove(key);
            }
            drop(maps);
        }
        return None;
    }

    pub fn insert(&self, key: &String, value: DataType, expire_time: Option<SystemTime>) {
        let mut maps = self.maps.write().unwrap();
        maps.data.insert(key.to_owned(), value);
        if expire_time.is_some() {
            maps.expire_times
                .insert(key.to_owned(), expire_time.unwrap());
        } else {
            maps.expire_times.remove(key);
        }
        drop(maps);
    }

    pub fn keys(&self) -> Vec<String> {
        let maps = self.maps.read().unwrap();
        let mut key_refs: Vec<&String> = maps.data.keys().collect();
        let now = SystemTime::now();
        key_refs.retain(|key| {
            !maps.expire_times.contains_key(*key) || !now.gt(maps.expire_times.get(*key).unwrap())
        });
        let keys: Vec<String> = key_refs
            .into_iter()
            .map(|key_ref| key_ref.to_owned())
            .collect();
        drop(maps);
        return keys;
    }

    pub fn reserve(&self, size: usize) {
        let mut maps = self.maps.write().unwrap();
        maps.data.reserve(size);
        maps.expire_times.reserve(size);
    }

    pub fn xadd(&self, key: &String, id: &String, fields: &[String]) -> Result<String, String> {
        let mut maps = self.maps.write().unwrap();
        if !maps.data.contains_key(key) {
            maps.data
                .insert(key.clone(), DataType::Stream(RedisStream::new()));
            maps.expire_times.remove(key);
        }

        let mut data = maps.data.get_mut(key).unwrap();
        let value = match &mut data {
            DataType::Stream(value) => Some(value),
            _ => None,
        }
        .unwrap();

        let mut entry = Vec::new();
        for i in (0..((fields.len() / 2) * 2) - 1).step_by(2) {
            entry.push((fields[i].clone(), fields[i + 1].clone()));
        }

        let result = value.insert(id.clone(), entry);
        drop(maps);
        return result;
    }

    pub fn xrange(&self, key: &String, start: &String, end: &String) -> Vec<RedisType> {
        let mut result: Vec<RedisType> = Vec::new();
        let maps = self.maps.read().unwrap();
        if maps.data.contains_key(key) {
            let value = match &maps.data.get(key).unwrap() {
                DataType::Stream(value) => Some(value),
                _ => None,
            }
            .unwrap();
            result = convert_entries_to_vec(value.query_inclusive(start, end));
        }
        return result;
    }

    pub fn xreadids(&self, keys: &Vec<String>, ids: &mut Vec<String>) {
        if ids.iter().find(|id| *id == "$").is_some() {
            // maybe make this into it's own function 'xids' or something
            let maps = self.maps.read().unwrap();
            for (i, key) in keys.iter().enumerate() {
                if ids[i] != "$" {
                    continue;
                }
                if maps.data.contains_key(key) {
                    let value = match &maps.data.get(key).unwrap() {
                        DataType::Stream(value) => Some(value),
                        _ => None,
                    }
                    .unwrap();
                    ids[i] = value.last_id();
                }
            }
            drop(maps);
        }
    }

    pub fn xread(&self, keys: &Vec<String>, ids: &Vec<String>) -> Vec<RedisType> {
        let mut result: Vec<RedisType> = Vec::new();
        let maps = self.maps.read().unwrap();
        for (i, key) in keys.iter().enumerate() {
            if maps.data.contains_key(key) {
                let mut stream_result = Vec::new();
                let value = match &maps.data.get(key).unwrap() {
                    DataType::Stream(value) => Some(value),
                    _ => None,
                }
                .unwrap();
                let entries =
                    convert_entries_to_vec(value.query_exclusive(&ids[i], &"+".to_owned()));
                if !entries.is_empty() {
                    stream_result.push(RedisType::BulkString(Some(key.clone())));
                    stream_result.push(RedisType::Array(entries));
                    result.push(RedisType::Array(stream_result));
                }
            }
        }
        drop(maps);
        return result;
    }
}

fn convert_entries_to_vec(entries: Vec<(&String, &Vec<(String, String)>)>) -> Vec<RedisType> {
    let mut result = Vec::new();
    for (id, entry) in entries {
        let mut entry_vec: Vec<RedisType> = Vec::new();
        entry_vec.push(RedisType::BulkString(Some(id.clone())));

        let mut fields_vec: Vec<RedisType> = Vec::new();
        for (key, value) in entry {
            fields_vec.push(RedisType::BulkString(Some(key.clone())));
            fields_vec.push(RedisType::BulkString(Some(value.clone())));
        }
        entry_vec.push(RedisType::Array(fields_vec));

        result.push(RedisType::Array(entry_vec));
    }
    return result;
}
