use chrono::Local;

pub fn next_id() -> u64 {
    Local::now().timestamp_millis() as u64
}
