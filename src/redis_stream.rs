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

    pub fn insert(&mut self, id: String, value: HashMap<String, String>) -> Result<(), String> {
        let parts: Vec<&str> = id.split('-').collect();
        if parts.len() != 2 {
            return Err("ERR Id had the incorrect ammount of parts".to_owned());
        }

        let milliseconds_time_result = str::parse::<u64>(parts[0]);
        let sequence_number_result = str::parse::<u64>(parts[1]);
        if milliseconds_time_result.is_err() || sequence_number_result.is_err() {
            return Err("ERR Id parts were not numbers".to_owned());
        }

        let milliseconds_time = milliseconds_time_result.unwrap();
        let sequence_number = sequence_number_result.unwrap();

        if milliseconds_time == 0 && sequence_number == 0 {
            return Err("ERR The ID specified in XADD must be greater than 0-0".to_owned());
        }

        if milliseconds_time < self.last_milliseconds_time
            || (milliseconds_time == self.last_milliseconds_time
                && sequence_number <= self.last_sequence_number)
        {
            return Err(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_owned(),
            );
        }

        self.last_milliseconds_time = milliseconds_time;
        self.last_sequence_number = sequence_number;
        self.data.insert(id, value);

        return Ok(());
    }
}
