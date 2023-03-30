#[cfg(feature = "memory")]
mod memory;
#[cfg(feature = "persist")]
mod persist;

#[cfg(feature = "memory")]
pub use memory::{CursorStorage, Error, TopicStorage};
#[cfg(feature = "persist")]
pub use persist::{CursorStorage, Error, TopicStorage};
