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

use observe::Observable;
use observe::Observer;
use observe::Subscription;
use observe::UpdatesSubscription;

use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use serde_json::from_str;
use serde_json::to_string;

use std::fmt::Debug;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::{spawn, JoinHandle};

/// The receiving end of a TCP channel has an address
/// and streams data to an observer.
#[derive(Debug)]
pub struct TcpReceiver<T> {
    addr: SocketAddr,
    observer: Arc<Mutex<Option<Box<dyn Observer<T, String> + Send>>>>,
    stream: Arc<Mutex<Option<TcpStream>>>,
}

impl<T: DeserializeOwned + Send + Debug + 'static> TcpReceiver<T> {
    /// Create a new TCP receiver with no observer. The receiver is
    /// inactive on creation, so to receive data the user must call
    /// `connect` to start a connection; and `listen` to listen for
    /// incoming data.
    ///
    /// `addr` may have a port set (by setting it to 0). In such a case
    /// the system will assign a port that is free. To retrieve this
    /// assigned port (in the form of the full `SocketAddr`), use the
    /// `addr` method.
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            observer: Arc::new(Mutex::new(None)),
            stream: Arc::new(Mutex::new(None)),
        }
    }

    /// Bind to the TCP address and start accepting connections.
    /// TODO handle errors instead of unwrapping
    pub fn connect(&mut self) -> JoinHandle<()> {
        let listener = TcpListener::bind(self.addr).unwrap();
        let stream = self.stream.clone();

        // We want to allow for auto-assigned ports, by letting the user
        // specify a `SocketAddr` with port 0. In this case, after
        // actually binding to an address, we need to update the port we
        // got assigned in `addr`, but for simplicity we just copy the
        // entire thing.
        self.addr = listener.local_addr().unwrap();

        spawn(move || {
            let (s, _) = listener.accept().unwrap();
            let mut stream = stream.lock().unwrap();
            *stream = Some(s);
        })
    }

    /// Listen to incoming data and pass it on to the observer.
    pub fn listen(&mut self) -> JoinHandle<Result<(), String>> {
        if let Some(stream) = self.stream.lock().unwrap().take() {
            let observer = self.observer.clone();
            spawn(move || {
                let mut observer = observer.lock().unwrap();
                if let Some(ref mut observer) = *observer {
                    let reader = BufReader::new(stream);
                    let mut lines = reader.lines().map(Result::unwrap);
                    loop {
                        let mut upds = lines
                            .by_ref()
                            .take_while(|line| line != "commit")
                            .map(|line| {
                                let v: T = from_str(&line).unwrap();
                                v
                            })
                            .peekable();
                        if upds.peek().is_none() {
                            break;
                        };
                        observer.on_start()?;
                        observer.on_updates(Box::new(upds))?;
                        observer.on_commit()?;
                    }
                }
                Ok(())
            })
        } else {
            spawn(|| Ok(()))
        }
    }

    /// Disconnect the receiver.
    pub fn disconnect(&mut self) {
        // Drop TCP stream to close connection
        let _ = self.stream.lock().unwrap().take();
    }

    /// Retrieve the address we are listening on.
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
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

    /// Disconnect the sender.
    pub fn disconnect(&mut self) {
        // Drop TCP stream to close connection
        let _ = self.stream.take();
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
