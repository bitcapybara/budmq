#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum SubType {
    /// Each subscription is only allowed to contain one client
    Exclusive = 1,
    /// Each subscription allows multiple clients
    Shared,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum InitialPostion {
    Latest = 1,
    Earliest,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
pub enum AccessMode {
    Exclusive = 1,
    Shared,
}