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

pub struct TcpReceiver<T> {
    addr: SocketAddr,
    observer: Arc<Mutex<Option<Box<dyn Observer<T, String> + Send>>>>
}

impl <T: DeserializeOwned + Send + Debug + 'static> TcpReceiver<T> {
    pub fn new(addr: SocketAddr) -> Self {
        TcpReceiver {
            addr: addr,
            observer: Arc::new(Mutex::new(None))
        }
    }

    pub fn listen(&mut self) -> JoinHandle<Result<(), String>> {
        let listener = TcpListener::bind(self.addr).unwrap();
        let observer = self.observer.clone();
        spawn(move || {
            let mut observer = observer.lock().unwrap();
            if let Some(ref mut observer) = *observer {
                let (stream, _) = listener.accept().unwrap();
                let reader = BufReader::new(stream);
                let upds = reader.lines().map(|line| {
                    let v: T = from_str(&line.unwrap()).unwrap();
                    v
                });
                observer.on_start()?;
                observer.on_updates(Box::new(upds))?;
                observer.on_commit()?;
            }
            Ok(())
        })
    }
}

struct TcpSubscription<T> {
    observer: Arc<Mutex<Option<Box<dyn Observer<T, String> + Send>>>>
}

impl <T> Subscription for TcpSubscription<T> {
    fn unsubscribe(self: Box<Self>) {
        let obs = self.observer.clone();
        let mut obs = obs.lock().unwrap();
        *obs = None;
    }
}

impl <T: 'static+ Send> Observable<T, String> for TcpReceiver<T> {
    fn subscribe(&mut self, observer: Box<dyn Observer<T, String> + Send>) -> Box<dyn Subscription> {
        let obs = self.observer.clone();
        let mut obs = obs.lock().unwrap();
        *obs = Some(observer);

        Box::new(TcpSubscription {
            observer: self.observer.clone()
        })
    }
}


pub struct TcpSender {
    addr: SocketAddr,
    stream: Option<TcpStream>,
}

impl TcpSender {
    pub fn new(socket: SocketAddr) -> Self {
        TcpSender {
            addr: socket,
            stream: None
        }
    }
}

impl<T: Serialize + Send> Observer<T, String> for TcpSender {
    fn on_start(&mut self) -> Result<(), String> {
        if let None = &self.stream {
            self.stream = Some(
                TcpStream::connect(self.addr).unwrap()
            );
        } else {
            panic!("Attempting to start another transaction \
                    while one is already in progress");
        }
        Ok(())
    }

    fn on_next(&mut self, upd: T) -> Result<(), String> {
        if let Some(ref mut stream) = self.stream {
            let s = to_string(&upd).unwrap() + "\n";
            stream.write(s.as_bytes())
                .map_err(|e| format!("{:?}", e))?;
        }
        Ok(())
    }

    // Write the updates to the TCP stream
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

    // Flush the TCP stream
    fn on_commit(&mut self) -> Result<(), String> {
        if let Some(ref mut stream) = self.stream {
            stream.flush()
                .map_err(|e| format!("{:?}", e))?;
        }
        Ok(())
    }

    // Close the TCP connection
    fn on_completed(&mut self) -> Result<(), String> {
        // Move and drop the TcpStream to close the connection
        self.stream.take();
        Ok(())
    }

    fn on_error(&self, _error: String) {}
}
