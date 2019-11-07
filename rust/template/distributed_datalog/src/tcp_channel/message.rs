use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result;

use serde::Deserialize;
use serde::Serialize;

/// An enum used for representing (and serializing/deserializing)
/// messages sent through the channel.
#[derive(Debug, Deserialize, Serialize)]
pub enum Message<T> {
    Start,
    Updates(Vec<T>),
    Commit,
    Complete,
}

impl<T> Display for Message<T> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> Result {
        let s = match self {
            Message::Start => "on_start",
            Message::Updates(_) => "on_updates",
            Message::Commit => "on_commit",
            Message::Complete => "on_completed",
        };
        formatter.write_str(s)
    }
}
