use std::io;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;

use observe::Observer;

use bincode::serialize_into;
use serde::Serialize;

use crate::message::Message;

/// The sending end of a TCP channel with a specified address and a TCP
/// connection.
#[derive(Debug)]
pub struct TcpSender {
    stream: TcpStream,
}

impl TcpSender {
    /// Create a new `TcpSender`, connecting to the given address.
    pub fn new<A>(addr: A) -> io::Result<Self>
    where
        A: ToSocketAddrs,
    {
        Ok(Self {
            stream: TcpStream::connect(addr)?,
        })
    }
}

impl<T> Observer<T, String> for TcpSender
where
    T: Serialize + Send,
{
    /// Perform some action before data start coming in.
    fn on_start(&mut self) -> Result<(), String> {
        serialize_into(&mut self.stream, &Message::<T>::Start).map_err(|e| e.to_string())
    }

    /// Send a series of items over the TCP channel.
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), String> {
        let message = Message::Updates(updates.collect());
        serialize_into(&mut self.stream, &message).map_err(|e| e.to_string())
    }

    /// Flush the TCP stream and signal the commit.
    fn on_commit(&mut self) -> Result<(), String> {
        serialize_into(&mut self.stream, &Message::<T>::Commit).map_err(|e| e.to_string())?;
        self.stream.flush().map_err(|e| e.to_string())
    }

    fn on_completed(&mut self) -> Result<(), String> {
        serialize_into(&mut self.stream, &Message::<T>::Complete).map_err(|e| e.to_string())?;
        self.stream.flush().map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::TcpReceiver;

    /// Connect a `TcpSender` to a `TcpReceiver`.
    #[test]
    fn connect() {
        let recv = TcpReceiver::<()>::new("127.0.0.1:0").unwrap();
        let _ = TcpSender::new(recv.addr()).unwrap();
    }
}
