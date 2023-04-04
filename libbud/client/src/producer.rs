type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub struct Producer {}

impl Producer {
    pub fn new(topic: &str) -> Self {
        todo!()
    }

    pub fn send(data: &[u8]) -> Result<()> {
        todo!()
    }
}
