use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    str::from_utf8,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use crate::{utils::byte_stream, Data, DataType};

fn read_length(stream: &mut impl Read) -> (u32, bool) {
    let first_byte = byte_stream::read_byte(stream).unwrap();
    match (first_byte >> 6) & 0b11 {
        0b00 => {
            return (first_byte as u32, false);
        }
        0b01 => {
            let second_byte = byte_stream::read_byte(stream).unwrap();
            let length = (((first_byte & 0b111111) as u32) << 8) | second_byte as u32;
            return (length, false);
        }
        0b10 => {
            let bytes = byte_stream::read_n_bytes(stream, 4).unwrap();

            let length = ((bytes[0] as u32) << 24)
                | ((bytes[1] as u32) << 16)
                | ((bytes[2] as u32) << 8)
                | (bytes[3] as u32);
            return (length, false);
        }
        0b11 => return (first_byte as u32 & 0b111111, true),
        _ => {}
    }
    return (0, false);
}

fn read_string(stream: &mut impl Read) -> String {
    let (length, encoded) = read_length(stream);
    if encoded {
        match length {
            0 => return (byte_stream::read_byte(stream).unwrap() as i8).to_string(),
            1 => {
                let bytes = byte_stream::read_n_bytes(stream, 2).unwrap();
                let mut integer: u16 = 0;
                for (i, byte) in bytes.iter().enumerate() {
                    integer |= (*byte as u16) << (i * 8);
                }
                return (integer as i16).to_string();
            }
            2 => {
                let bytes = byte_stream::read_n_bytes(stream, 4).unwrap();
                let mut integer: u32 = 0;
                for (i, byte) in bytes.iter().enumerate() {
                    integer |= (*byte as u32) << (i * 8);
                }
                return (integer as i32).to_string();
            }
            _ => return "".to_owned(),
        }
    }
    let bytes = byte_stream::read_n_bytes(stream, length as usize).unwrap();
    return from_utf8(&bytes).unwrap().to_owned();
}

fn read_key_value_pair(
    stream: &mut impl Read,
    value_type_option: Option<u8>,
) -> Option<(String, String)> {
    let value_type = value_type_option.unwrap_or_else(|| byte_stream::read_byte(stream).unwrap());
    match value_type {
        // string encoding
        0 => {
            let key = read_string(stream);
            let value = read_string(stream);
            return Some((key, value));
        }
        // TODO support more than string encoding
        _ => {
            return None;
        }
    }
}

pub fn load_rdb(filepath: String, data_store: &Arc<RwLock<HashMap<String, Data>>>) {
    let file_result = File::open(filepath);
    if file_result.is_err() {
        return;
    }
    let mut file = file_result.unwrap();
    byte_stream::read_n_bytes(&mut file, 9).unwrap(); // REDISvvvv (v) version

    loop {
        let opcode = byte_stream::read_byte(&mut file).unwrap();
        match opcode {
            // EOF
            0xff => {
                return;
            }
            // SELECTDB
            0xfe => {
                read_length(&mut file); // db id
            }
            // EXPIRETIME
            0xfd => {
                let bytes = byte_stream::read_n_bytes(&mut file, 4).unwrap();
                let mut expire_time_s: u32 = 0;
                for (i, byte) in bytes.iter().enumerate() {
                    expire_time_s |= (*byte as u32) << (i * 8);
                }
                let (key, value) = read_key_value_pair(&mut file, None).unwrap();

                let mut map = data_store.write().unwrap();
                map.insert(
                    key,
                    Data {
                        value: DataType::String(value),
                        expire_time: Some(
                            SystemTime::UNIX_EPOCH + Duration::from_secs(expire_time_s as u64),
                        ),
                    },
                );
                drop(map);
            }
            // EXPIRETIMEMS
            0xfc => {
                let bytes = byte_stream::read_n_bytes(&mut file, 8).unwrap();
                let mut expire_time_ms: u64 = 0;
                for (i, byte) in bytes.iter().enumerate() {
                    expire_time_ms |= (*byte as u64) << (i * 8);
                }
                let (key, value) = read_key_value_pair(&mut file, None).unwrap();

                let mut map = data_store.write().unwrap();
                map.insert(
                    key,
                    Data {
                        value: DataType::String(value),
                        expire_time: Some(
                            SystemTime::UNIX_EPOCH + Duration::from_millis(expire_time_ms),
                        ),
                    },
                );
                drop(map);
            }
            // RESIZEDB
            0xfb => {
                let (map_size, _) = read_length(&mut file);
                read_length(&mut file); // expire size

                let mut map = data_store.write().unwrap();
                map.reserve(map_size as usize);
                drop(map);
            }
            // AUX
            0xfa => {
                read_string(&mut file); // key
                read_string(&mut file); // value
            }
            // key value pair
            _ => {
                let (key, value) = read_key_value_pair(&mut file, Some(opcode)).unwrap();
                let mut map = data_store.write().unwrap();
                map.insert(
                    key,
                    Data {
                        value: DataType::String(value),
                        expire_time: None,
                    },
                );
                drop(map);
            }
        }
    }
}
