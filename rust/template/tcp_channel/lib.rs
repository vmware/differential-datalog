//! TCP implementation of an Observer/Observable channel. To establish
//! a network of communication, the receiver on each node must be
//! connected to its address (TcpListener::connect) before any sender
//! tries to connect to it (TcpSender::connect).

#![allow(clippy::type_complexity)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs
)]

use std::fmt::Debug;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::ops::DerefMut;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::spawn;
use std::thread::JoinHandle;

use libc::close;

use observe::Observable;
use observe::Observer;
use observe::Subscription;
use observe::UpdatesSubscription;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json::from_str;
use serde_json::to_string;

/// The receiving end of a TCP channel has an address
/// and streams data to an observer.
#[derive(Debug)]
pub struct TcpReceiver<T> {
    addr: SocketAddr,
    listener: RawFd,
    stream: Arc<Mutex<Option<RawFd>>>,
    thread: JoinHandle<io::Result<()>>,
    observer: Arc<Mutex<Option<Box<dyn Observer<T, String> + Send>>>>,
}

impl<T: DeserializeOwned + Send + Debug + 'static> TcpReceiver<T> {
    /// Create a new TCP receiver with no observer.
    ///
    /// `addr` may have a port set (by setting it to 0). In such a case
    /// the system will assign a port that is free. To retrieve this
    /// assigned port (in the form of the full `SocketAddr`), use the
    /// `addr` method.
    pub fn new<A>(addr: A) -> io::Result<Self>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr)?;
        // We want to allow for auto-assigned ports, by letting the user
        // specify a `SocketAddr` with port 0. In this case, after
        // actually binding to an address, we need to update the port we
        // got assigned in `addr`, but for simplicity we just copy the
        // entire thing.
        let addr = listener.local_addr()?;
        let listener_fd = listener.as_raw_fd();
        let stream_fd = Arc::new(Mutex::new(None));
        let observer = Arc::new(Mutex::new(None));
        let thread = Self::accept(stream_fd.clone(), listener, observer.clone());

        Ok(Self {
            addr,
            listener: listener_fd,
            stream: stream_fd,
            thread,
            observer,
        })
    }

    /// Accept a connection (in a non-blocking manner), read data from
    /// it, and dispatch that to the subscribed observer, if any. If no
    /// observer is subscribed, data will be silently dropped.
    fn accept(
        stream: Arc<Mutex<Option<RawFd>>>,
        listener: TcpListener,
        observer: Arc<Mutex<Option<Box<dyn Observer<T, String> + Send>>>>,
    ) -> JoinHandle<io::Result<()>> {
        spawn(move || {
            let (socket, _) = listener.accept()?;
            *stream.lock().unwrap() = Some(socket.as_raw_fd());

            let reader = BufReader::new(socket);
            let mut lines = reader.lines();
            loop {
                let mut upds = lines
                    .by_ref()
                    .take_while(|result| match result {
                        Ok(line) if line == "commit" => false,
                        Ok(_) => true,
                        Err(_) => false,
                    })
                    .map(|result| {
                        result.map(|line| {
                            let v: T = from_str(&line).unwrap();
                            v
                        })
                    });

                let first = match upds.next() {
                    None => break,
                    Some(v @ Ok(_)) => v,
                    Some(Err(e)) => return Err(e),
                };

                // If there is no observer we just drop the data, which
                // is seemingly the only reasonable behavior given that
                // observers can come and go by virtue of our API
                // design.
                if let Some(ref mut observer) = observer.lock().unwrap().deref_mut() {
                    let upds = Some(first).into_iter().chain(upds).map(Result::unwrap);
                    // TODO: Need to handle those errors eventually (or
                    //       perhaps we will end up with method
                    //       signatures that don't allow for errors?).
                    observer.on_start().unwrap();
                    observer.on_updates(Box::new(upds)).unwrap();
                    observer.on_commit().unwrap();
                }
            }
            Ok(())
        })
    }

    /// Retrieve the address we are listening on.
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }
}

impl<T> Drop for TcpReceiver<T> {
    fn drop(&mut self) {
        // We can't really handle any errors here.
        // The order of operations is important. First close the
        // listener to be sure that a no new stream will be accepted,
        // then close the stream itself.
        unsafe {
            let _ = close(self.listener);
            let _ = self.stream.lock().unwrap().map(|fd| close(fd));
        }
        // The remaining members will be destroyed automatically, no
        // need to bother here.
    }
}

impl<T> Observable<T, String> for TcpReceiver<T>
where
    T: Debug + Send + 'static,
{
    /// An observer subscribes to the receiving end of a TCP channel to
    /// listen to incoming data.
    fn subscribe(
        &mut self,
        observer: Box<dyn Observer<T, String> + Send>,
    ) -> Box<dyn Subscription> {
        let mut guard = self.observer.lock().unwrap();
        match *guard {
            Some(_) => panic!(
                "TcpReceiver {} already has an observer subscribed",
                self.addr
            ),
            None => *guard = Some(observer),
        }

        Box::new(UpdatesSubscription::<T, String> {
            observer: self.observer.clone(),
        })
    }
}

/// The sending end of a TCP channel with a specified address and a TCP
/// connection.
#[derive(Debug)]
pub struct TcpSender {
    addr: SocketAddr,
    stream: Option<TcpStream>,
}

impl TcpSender {
    /// Create a new `TcpSender` but without starting the connection.
    pub fn new(socket: SocketAddr) -> Self {
        Self {
            addr: socket,
            stream: None,
        }
    }

    /// Connect to the specified address. Repeat connection attempt if
    /// the receiver is not ready (ConnectionRefused).
    pub fn connect(&mut self) -> Result<(), String> {
        if self.stream.is_none() {
            loop {
                match TcpStream::connect(self.addr) {
                    Ok(c) => {
                        self.stream = Some(c);
                        break;
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::ConnectionRefused {
                            continue;
                        } else {
                            panic!("TCP connection failed")
                        }
                    }
                }
            }
        } else {
            panic!(
                "Attempting to start another transaction \
                 while one is already in progress"
            );
        }
        Ok(())
    }
}

impl<T: Serialize + Send> Observer<T, String> for TcpSender {
    /// Perform some action before data start coming in.
    fn on_start(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Send a series of items over the TCP channel.
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), String> {
        if let Some(ref mut stream) = self.stream {
            for upd in updates {
                let s = to_string(&upd).unwrap() + "\n";
                stream.write(s.as_bytes()).map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }

    /// Flush the TCP stream and signal the commit.
    /// TODO writing "commit" to stream feels like a hack
    fn on_commit(&mut self) -> Result<(), String> {
        if let Some(ref mut stream) = self.stream {
            stream.write_all(b"commit\n").map_err(|e| e.to_string())?;
            stream.flush().map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn on_completed(&mut self) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::ErrorKind;

    /// Connect to a `TcpReceiver`.
    #[test]
    fn receiver_accept() {
        let recv = TcpReceiver::<()>::new("127.0.0.1:0").unwrap();
        let _ = TcpStream::connect(recv.addr()).unwrap();
    }

    /// Check that the listener socket is cleaned up properly when a
    /// `TcpReceiver` is dropped but has never accepted a connection.
    #[test]
    fn receiver_never_accepted() {
        let addr = {
            let recv = TcpReceiver::<()>::new("127.0.0.1:0").unwrap();
            recv.addr().clone()
        };

        let err = TcpStream::connect(addr).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConnectionRefused);
    }
}
