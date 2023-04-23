#![allow(dead_code)]

pub mod client;
mod connector;
pub mod consumer;
pub mod producer;
mod register;

use std::time::Duration;

pub(crate) const WAIT_REPLY_TIMEOUT: Duration = Duration::from_millis(200);
