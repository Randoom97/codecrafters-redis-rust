use std::collections::HashMap;

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

    pub fn insert(&mut self, id: String, value: HashMap<String, String>) -> Result<String, String> {
        if id == "0-0" {
            return Err("ERR The ID specified in XADD must be greater than 0-0".to_owned());
        }

        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() != 2 {
            return Err("ERR Id had the incorrect ammount of parts".to_owned());
        }

        let milliseconds_time = str::parse::<u64>(parts[0]).unwrap();
        if milliseconds_time < self.last_milliseconds_time {
            return Err(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_owned(),
            );
        }

        let mut sequence_number = 0;
        if parts[1] == "*" {
            if milliseconds_time == self.last_milliseconds_time {
                sequence_number = self.last_sequence_number + 1;
            }
        } else {
            sequence_number = str::parse::<u64>(parts[1]).unwrap();

            if milliseconds_time == self.last_milliseconds_time
                && sequence_number <= self.last_sequence_number
            {
                return Err(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                        .to_owned(),
                );
            }
        }

        self.last_milliseconds_time = milliseconds_time;
        self.last_sequence_number = sequence_number;
        self.data.insert(id, value);

        return Ok(format!("{milliseconds_time}-{sequence_number}"));
    }
}
