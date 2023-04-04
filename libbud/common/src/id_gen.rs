type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

/// 64 bits id generator
///
/// bits assignment:
/// * timestamp(42 bits)
/// * machineId(4 bits)
/// * sequence(18 bits)
pub struct IdGenerator {}

impl IdGenerator {
    /// machine id max value is 15
    pub fn new(machine_id: u8) -> Result<Self> {
        todo!()
    }

    pub fn next() -> u64 {
        todo!()
    }
}
