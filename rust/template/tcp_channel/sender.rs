use std::fmt::Debug;
use std::io;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;

use observe::Observer;

use serde::ser::Serialize;
use serde_json::to_string;

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

impl<T: Serialize + Send> Observer<T, String> for TcpSender {
    /// Perform some action before data start coming in.
    fn on_start(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Send a series of items over the TCP channel.
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), String> {
        for upd in updates {
            let s = to_string(&upd).unwrap() + "\n";
            self.stream.write(s.as_bytes()).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Flush the TCP stream and signal the commit.
    /// TODO writing "commit" to stream feels like a hack
    fn on_commit(&mut self) -> Result<(), String> {
        self.stream
            .write_all(b"commit\n")
            .map_err(|e| e.to_string())?;
        self.stream.flush().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn on_completed(&mut self) -> Result<(), String> {
        Ok(())
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
