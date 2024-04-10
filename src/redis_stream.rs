use std::{collections::HashMap, time::SystemTime};

#[derive(Debug)]
pub struct RedisStream {
    last_milliseconds_time: u64,
    last_sequence_number: u64,
    data: HashMap<String, HashMap<String, String>>,
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
        value: HashMap<String, String>,
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
}
