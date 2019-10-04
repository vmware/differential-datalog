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
