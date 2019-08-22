// TCP implementation of an Observer/Observable channel. To establish
// a network of communication, the receiver on each node must be
// connected to its address (TcpListener::connect) before any sender
// tries to connect to it (TcpSender::connect).

extern crate serde_json;

use observe::{Observer, Observable, Subscription};

use serde::ser::Serialize;
use serde::de::DeserializeOwned;
use serde_json::from_str;
use serde_json::ser::to_string;

use std::net::{TcpStream, TcpListener, SocketAddr};
use std::io::prelude::*;
use std::io::BufReader;
use std::sync::{Arc, Mutex};
use std::thread::{spawn, JoinHandle};
use std::fmt::Debug;

// The receiving end of a TCP channel has an address
// and streams data to an observer.
pub struct TcpReceiver<T> {
    addr: SocketAddr,
    observer: Arc<Mutex<Option<Box<dyn Observer<T, String> + Send>>>>,
    stream: Arc<Mutex<Option<TcpStream>>>
}

impl <T: DeserializeOwned + Send + Debug + 'static> TcpReceiver<T> {
    // Create a new TCP receiver with no observer. The receiver is
    // inactive on creation, so to receive data the user must call
    // connect to start a connection; and listen to listen for
    // incoming data
    pub fn new(addr: SocketAddr) -> Self {
        TcpReceiver {
            addr: addr,
            observer: Arc::new(Mutex::new(None)),
            stream: Arc::new(Mutex::new(None))
        }
    }

    // Bind to the TCP address and start accepting connections
    // TODO handle errors instead of unwrapping
    pub fn connect(&mut self) -> JoinHandle<()> {
        let listener = TcpListener::bind(self.addr).unwrap();
        let stream = self.stream.clone();
        spawn(move || {
            let (s, _) = listener.accept().unwrap();
            let mut stream = stream.lock().unwrap();
            *stream = Some(s);
        })
    }

    // Listen to incoming data and pass it on to the observer
    pub fn listen(&mut self) -> JoinHandle<Result<(), String>> {
        if let Some(stream) = self.stream.lock().unwrap().take() {
            let observer = self.observer.clone();
            spawn(move || {
                let mut observer = observer.lock().unwrap();
                if let Some(ref mut observer) = *observer {
                    let reader = BufReader::new(stream);
                    let mut lines = reader.lines().map(|line| line.unwrap());
                    loop {
                        let mut upds = lines
                            .by_ref()
                            .take_while(|line| line != "commit")
                            .map(|line| {
                                let v: T = from_str(&line).unwrap();
                                v
                            }).peekable();
                        if upds.peek().is_none() {break};
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

    pub fn disconnect(&mut self) {
        // Drop TCP stream to close connection
        let _ = self.stream.lock().unwrap().take();
    }
}

pub struct ATcpSender (pub Arc<Mutex<TcpSender>>);

impl <T: Send + Serialize> Observer<T, String> for ATcpSender {
    fn on_start(&mut self) -> Result<(), String> {
        let mut s = self.0.lock().unwrap();
        Observer::<T, String>::on_start(&mut *s)
    }

    fn on_commit(&mut self) -> Result<(), String> {
        let mut s = self.0.lock().unwrap();
        Observer::<T, String>::on_commit(&mut *s)
    }

    fn on_next(&mut self, upd: T) -> Result<(), String> {
        self.0.lock().unwrap().on_next(upd)
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), String> {
        self.0.lock().unwrap().on_updates(updates)
    }

    fn on_error(&self, error: String) {
        let mut s = self.0.lock().unwrap();
        Observer::<T, String>::on_error(&mut *s, error)
    }

    fn on_completed(&mut self) -> Result<(), String> {
        let mut s = self.0.lock().unwrap();
        Observer::<T, String>::on_completed(&mut *s)
    }
}

struct TcpSubscription<T> {
    // Points to the observer field of a TcpReceiver (set to None
    // upon unsubscribe)
    observer: Arc<Mutex<Option<Box<dyn Observer<T, String> + Send>>>>
}

impl <T> Subscription for TcpSubscription<T> {
    // Cancel the subscription so that the observer stops receiving data
    fn unsubscribe(self: Box<Self>) {
        let mut observer = self.observer.lock().unwrap();
        *observer = None;
    }
}

impl <T: 'static+ Send> Observable<T, String> for TcpReceiver<T> {
    // An observer subscribes to the receiving end of a TCP channel to
    // listen to incoming data
    fn subscribe(&mut self, observer: Box<dyn Observer<T, String> + Send>) -> Box<dyn Subscription> {
        let mut obs = self.observer.lock().unwrap();
        *obs = Some(observer);

        Box::new(TcpSubscription {
            observer: self.observer.clone()
        })
    }
}

// The sending end of a TCP channel with a specified address
// and a TCP connection
pub struct TcpSender {
    addr: SocketAddr,
    stream: Option<TcpStream>,
}

impl TcpSender {
    // Create a new TcpSender but without starting the connection
    pub fn new(socket: SocketAddr) -> Self {
        TcpSender {
            addr: socket,
            stream: None,
        }
    }

    // Connect to the specified address. Repeat connection attempt
    // if the receiver is not ready (ConnectionRefused)
    pub fn connect(&mut self) -> Result<(), String> {
        if let None = &self.stream {
            loop {
                match TcpStream::connect(self.addr) {
                    Ok(c) => {
                        self.stream = Some(c);
                        break
                    },
                    Err(e) => if e.kind() == std::io::ErrorKind::ConnectionRefused {
                        continue
                    } else {
                        panic!("TCP connection failed")
                    }
                }
            }
        } else {
            panic!("Attempting to start another transaction \
                    while one is already in progress");
        }
        Ok(())
    }

    pub fn disconnect(&mut self) {
        // Drop TCP stream to close connection
        let _ = self.stream.take();
    }
}

impl<T: Serialize + Send> Observer<T, String> for TcpSender {
    // Connect to the specified TCP address
    fn on_start(&mut self) -> Result<(), String> {
        Ok(())
    }

    // Send a single item over the TCP channel
    fn on_next(&mut self, upd: T) -> Result<(), String> {
        if let Some(ref mut stream) = self.stream {
            let s = to_string(&upd).unwrap() + "\n";
            stream.write(s.as_bytes())
                .map_err(|e| format!("{:?}", e))?;
        }
        Ok(())
    }

    // Send a series of items over the TCP channel
    fn on_updates<'a>(&mut self,
                      updates: Box<dyn Iterator<Item = T> + 'a>)
                      -> Result<(), String> {
        if let Some(ref mut stream) = self.stream {
            for upd in updates {
                let s = to_string(&upd).unwrap() + "\n";
                stream.write(s.as_bytes())
                    .map_err(|e| format!("{:?}", e))?;
            }
        }
        Ok(())
    }

    // Flush the TCP stream and signal the commit
    // TODO writing "commit" to stream feels like a HACK
    fn on_commit(&mut self) -> Result<(), String> {
        if let Some(ref mut stream) = self.stream {
            stream.write_all(b"commit\n")
                .map_err(|e| format!("{:?}", e))?;
            stream.flush()
                .map_err(|e| format!("{:?}", e))?;
        }
        Ok(())
    }

    fn on_completed(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn on_error(&self, _error: String) {}
}
