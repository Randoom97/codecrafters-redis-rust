use std::{cmp::Ordering, collections::HashMap, time::SystemTime};

#[derive(Debug)]
pub struct RedisStream {
    last_milliseconds_time: u64,
    last_sequence_number: u64,
    data: HashMap<String, Vec<(String, String)>>,
}

impl RedisStream {
    pub fn new() -> RedisStream {
        return RedisStream {
            last_milliseconds_time: 0,
            last_sequence_number: 0,
            data: HashMap::new(),
        };
    }

    pub fn insert(
        &mut self,
        mut id: String,
        value: Vec<(String, String)>,
    ) -> Result<String, String> {
        if id == "0-0" {
            return Err("ERR The ID specified in XADD must be greater than 0-0".to_owned());
        }

        let mut generate_milliseconds_time = false;
        let mut generate_sequence_number = false;
        let mut milliseconds_time: u64 = 0;
        let mut sequence_number: u64 = 0;
        if id == "*" {
            generate_milliseconds_time = true;
            generate_sequence_number = true;
        } else {
            let parts: Vec<&str> = id.split('-').collect();
            if parts.len() != 2 {
                return Err("ERR Id had the incorrect ammount of parts".to_owned());
            }

            milliseconds_time = str::parse::<u64>(parts[0]).unwrap();
            if parts[1] == "*" {
                generate_sequence_number = true;
            } else {
                sequence_number = str::parse::<u64>(parts[1]).unwrap();
            }
        }

        if generate_milliseconds_time {
            milliseconds_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        }

        if milliseconds_time < self.last_milliseconds_time {
            return Err(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_owned(),
            );
        }

        if milliseconds_time == self.last_milliseconds_time {
            if generate_sequence_number {
                sequence_number = self.last_sequence_number + 1;
            } else if sequence_number <= self.last_sequence_number {
                return Err(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_owned(),
                );
            }
        }

        self.last_milliseconds_time = milliseconds_time;
        self.last_sequence_number = sequence_number;
        id = format!("{milliseconds_time}-{sequence_number}");
        self.data.insert(id.clone(), value);

        return Ok(id);
    }

    pub fn query(&self, start: &String, end: &String) -> Vec<(&String, &Vec<(String, String)>)> {
        let mut keys: Vec<&String> = self.data.keys().collect();
        keys.retain(|key| {
            if start != "-" {
                match compare_ids(&key, &start) {
                    Ordering::Less => return false,
                    _ => {}
                }
            }

            match compare_ids(&key, &end) {
                Ordering::Greater => return false,
                _ => {}
            }

            return true;
        });
        keys.sort_unstable_by(compare_ids);

        let mut result: Vec<(&String, &Vec<(String, String)>)> = Vec::new();
        for key in keys {
            result.push((key, self.data.get(key).unwrap()));
        }

        return result;
    }
}

fn compare_ids(a: &&String, b: &&String) -> Ordering {
    let a_parts: Vec<&str> = a.split('-').collect();
    let a_time = str::parse::<u64>(a_parts[0]).unwrap();
    let a_sequence = a_parts.get(1).and_then(|part| str::parse::<u64>(part).ok());

    let b_parts: Vec<&str> = b.split('-').collect();
    let b_time = str::parse::<u64>(b_parts[0]).unwrap();
    let b_sequence = b_parts.get(1).and_then(|part| str::parse::<u64>(part).ok());

    if a_time < b_time {
        return Ordering::Less;
    } else if a_time > b_time {
        return Ordering::Greater;
    }

    if a_sequence.is_some() && b_sequence.is_some() {
        if a_sequence.unwrap() < b_sequence.unwrap() {
            return Ordering::Less;
        } else if a_sequence.unwrap() > b_sequence.unwrap() {
            return Ordering::Greater;
        }
    }

    return Ordering::Equal;
}
