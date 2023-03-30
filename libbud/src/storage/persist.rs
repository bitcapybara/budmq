type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub struct TopicStorage {}

impl TopicStorage {
    pub fn new() -> Result<Self> {
        todo!()
    }
}

pub struct CursorStorage {}

impl CursorStorage {
    pub fn new() -> Result<Self> {
        todo!()
    }
}
