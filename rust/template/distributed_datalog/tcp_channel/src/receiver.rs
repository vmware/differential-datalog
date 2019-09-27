use std::fmt::Debug;
use std::io::BufReader;
use std::io::Error;
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

use bincode::deserialize_from;

use libc::shutdown;
use libc::SHUT_RDWR;

use log::error;

use observe::Observable;
use observe::ObserverBox;

use serde::de::DeserializeOwned;

use crate::message::Message;

#[derive(Copy, Clone, Debug)]
enum Fd {
    /// We are still listening for an incoming connection and
    /// this is the corresponding file descriptor.
    Listening(RawFd),
    /// We have accepted a connection and read data from it.
    Accepted(RawFd),
    /// The listener/accepted connection has been shutdown.
    Shutdown,
}

impl Fd {
    fn shutdown(&mut self) -> Result<(), Error> {
        match *self {
            Fd::Accepted(fd) | Fd::Listening(fd) => {
                // Assuming correctness on the receiver, there is no
                // good reason why a shutdown would fail and so if it
                // were to we are probably in some really awkward state
                // we shouldn't be in. To be able to recover eventually
                // (by cleaning up), let's at least allow the "acceptor"
                // thread to finish up by indicating that we are shut
                // down indeed unconditionally.
                *self = Fd::Shutdown;

                let rc = unsafe { shutdown(fd, SHUT_RDWR) };
                if rc != 0 {
                    return Err(Error::last_os_error());
                }
                Ok(())
            }
            Fd::Shutdown => Ok(()),
        }
    }
}

/// The receiving end of a TCP channel has an address
/// and streams data to an observer.
#[derive(Debug)]
pub struct TcpReceiver<T> {
    /// The address we are listening on.
    addr: SocketAddr,
    /// Our listener/connection file descriptor state; shared with the
    /// thread accepting connections and reading streamed data.
    fd: Arc<Mutex<Fd>>,
    /// Handle to the thread accepting a connection and processing data.
    thread: Option<JoinHandle<Result<(), String>>>,
    /// The connected observer, if any.
    observer: Arc<Mutex<Option<ObserverBox<T, String>>>>,
}

impl<T> TcpReceiver<T>
where
    T: DeserializeOwned + Send + Debug + 'static,
{
    /// Create a new TCP receiver with no observer.
    ///
    /// `addr` may have a port set (by setting it to 0). In such a case
    /// the system will assign a port that is free. To retrieve this
    /// assigned port (in the form of the full `SocketAddr`), use the
    /// `addr` method.
    pub fn new<A>(addr: A) -> Result<Self, String>
    where
        A: ToSocketAddrs,
    {
        let listener =
            TcpListener::bind(addr).map_err(|e| format!("failed to bind TCP socket: {}", e))?;
        // We want to allow for auto-assigned ports, by letting the user
        // specify a `SocketAddr` with port 0. In this case, after
        // actually binding to an address, we need to update the port we
        // got assigned in `addr`, but for simplicity we just copy the
        // entire thing.
        let addr = listener
            .local_addr()
            .map_err(|e| format!("failed to inquire local address: {}", e))?;
        let fd = Arc::new(Mutex::new(Fd::Listening(listener.as_raw_fd())));
        let observer = Arc::new(Mutex::new(None));
        let thread = Some(Self::accept(listener, fd.clone(), observer.clone()));

        Ok(Self {
            addr,
            fd,
            thread,
            observer,
        })
    }

    /// Accept a connection (in a non-blocking manner), read data from
    /// it, and dispatch that to the subscribed observer, if any. If no
    /// observer is subscribed, data will be silently dropped.
    fn accept(
        listener: TcpListener,
        fd: Arc<Mutex<Fd>>,
        observer: Arc<Mutex<Option<ObserverBox<T, String>>>>,
    ) -> JoinHandle<Result<(), String>> {
        spawn(move || {
            let result = listener.accept();
            let socket = {
                let mut guard = fd.lock().unwrap();
                // The user may have dropped the receiver shortly after
                // us accepting a connection. If that is the case do not
                // continue.
                if let Fd::Shutdown = *guard {
                    return Ok(());
                }

                let (socket, _) =
                    result.map_err(|e| format!("failed to accept connection: {}", e))?;
                *guard = Fd::Accepted(socket.as_raw_fd());
                socket
            };

            Self::process(socket, fd, observer)
        })
    }

    /// Process data from a `TcpSender`, relaying messages to a
    /// connected `Observer`, if any, or dropping them.
    fn process(
        socket: TcpStream,
        fd: Arc<Mutex<Fd>>,
        observer: Arc<Mutex<Option<ObserverBox<T, String>>>>,
    ) -> Result<(), String> {
        let mut reader = BufReader::new(socket);
        loop {
            let mut message = match deserialize_from(&mut reader) {
                Ok(m) => m,
                Err(e) => {
                    if let Fd::Shutdown = *fd.lock().unwrap() {
                        return Ok(());
                    }
                    error!("failed to deserialize message: {}", e);
                    continue;
                }
            };

            // If there is no observer we just drop the data, which
            // is seemingly the only reasonable behavior given that
            // observers can come and go by virtue of our API
            // design.
            if let Some(ref mut observer) = observer.lock().unwrap().deref_mut() {
                let result = match message {
                    Message::Start => observer.on_start(),
                    Message::Updates(ref mut updates) => {
                        observer.on_updates(Box::new(updates.drain(..)))
                    }
                    Message::Commit => observer.on_commit(),
                    Message::Complete => observer.on_completed(),
                };

                if let Err(e) = result {
                    error!(
                        "observer {:?} failed to process {} event: {}",
                        observer, message, e
                    );
                }
            }
        }
    }

    /// Retrieve the address we are listening on.
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }
}

impl<T> Drop for TcpReceiver<T> {
    fn drop(&mut self) {
        // Note that we only ever shut down the file descriptor, but
        // don't close it. The close will happen once the "acceptor"
        // thread wakes up, sees that we are shut down, and exits,
        // dropping the TcpListener/TcpStream in the process.
        if let Err(e) = self.fd.lock().unwrap().shutdown() {
            error!("failed to shut down TcpReceiver file descriptor: {}", e);
        }

        if let Some(t) = self.thread.take() {
            match t.join() {
                Ok(Ok(())) => (),
                Ok(Err(e)) => error!("TcpReceiver accept thread failed: {}", e),
                Err(e) => error!("TcpReceiver thread has panicked: {:?}", e),
            }
        };

        // The remaining members will be destroyed automatically, no
        // need to bother here.
    }
}

impl<T> Observable<T, String> for TcpReceiver<T>
where
    T: Debug + Send + 'static,
{
    type Subscription = ();

    /// An observer subscribes to the receiving end of a TCP channel to
    /// listen to incoming data.
    fn subscribe(&mut self, observer: ObserverBox<T, String>) -> Option<Self::Subscription> {
        let mut guard = self.observer.lock().unwrap();
        match *guard {
            Some(_) => None,
            None => {
                *guard = Some(observer);
                Some(())
            }
        }
    }

    /// Unsubscribe a previously subscribed `Observer` based on a
    /// subscription.
    fn unsubscribe(&mut self, _subscription: &Self::Subscription) -> bool {
        let mut guard = self.observer.lock().unwrap();
        match *guard {
            Some(_) => {
                *guard = None;
                true
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::ErrorKind;
    use std::net::TcpStream;

    use test_env_log::test;

    /// Drop a `TcpReceiver`.
    #[test]
    fn drop() {
        let _recv = TcpReceiver::<()>::new("127.0.0.1:0").unwrap();
    }

    /// Connect to a `TcpReceiver`.
    #[test]
    fn accept() {
        let recv = TcpReceiver::<()>::new("127.0.0.1:0").unwrap();
        {
            let _send = TcpStream::connect(recv.addr()).unwrap();
        }
    }

    /// Check that the listener socket is cleaned up properly when a
    /// `TcpReceiver` is dropped but has never accepted a connection.
    #[test]
    fn never_accepted() {
        let addr = {
            let recv = TcpReceiver::<()>::new("127.0.0.1:0").unwrap();
            recv.addr().clone()
        };

        let err = TcpStream::connect(addr).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConnectionRefused);
    }
}
