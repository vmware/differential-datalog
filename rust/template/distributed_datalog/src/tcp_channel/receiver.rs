use std::fmt::Debug;
use std::io::BufReader;
use std::io::Error;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
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

use serde::de::DeserializeOwned;

use crate::observe::Observable;
use crate::observe::Observer;
use crate::observe::ObserverBox;
use crate::observe::SharedObserver;
use crate::tcp_channel::message::Message;
use crate::txnmux::TxnMux;

/// A struct representing both an `Observer` and an `Observable` that
/// just passes observable events through to the inner observer.
#[derive(Debug)]
struct Passthrough<T, E>(Option<ObserverBox<T, E>>);

impl<T, E> Passthrough<T, E> {
    fn new() -> Self {
        Self(None)
    }
}

impl<T, E> Observable<T, E> for Passthrough<T, E>
where
    T: Debug + Send,
    E: Debug + Send,
{
    type Subscription = ();

    fn subscribe(
        &mut self,
        observer: ObserverBox<T, E>,
    ) -> Result<Self::Subscription, ObserverBox<T, E>> {
        assert!(
            !self.0.is_some(),
            "multiple subscriptions attempted on a Passthrough"
        );
        self.0 = Some(observer);
        Ok(())
    }

    fn unsubscribe(&mut self, _subscription: &Self::Subscription) -> Option<ObserverBox<T, E>> {
        self.0.take()
    }
}

impl<T, E> Observer<T, E> for Passthrough<T, E>
where
    T: Debug + Send,
    E: Debug + Send,
{
    fn on_start(&mut self) -> Result<(), E> {
        self.0.as_mut().map_or(Ok(()), |o| o.on_start())
    }

    fn on_commit(&mut self) -> Result<(), E> {
        self.0.as_mut().map_or(Ok(()), |o| o.on_commit())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = T> + 'a>) -> Result<(), E> {
        self.0.as_mut().map_or(Ok(()), |o| o.on_updates(updates))
    }

    fn on_completed(&mut self) -> Result<(), E> {
        self.0.as_mut().map_or(Ok(()), |o| o.on_completed())
    }
}

/// A struct representing a socket file descriptor that can be shut down
/// while an actual owned object (e.g., `TcpStream`) exists.
#[derive(Copy, Clone, Debug)]
enum Fd {
    /// The socket is still active.
    Active(RawFd),
    /// The socket has been shutdown.
    Shutdown,
}

impl Fd {
    fn shutdown(&mut self) -> Result<(), Error> {
        match *self {
            Fd::Active(fd) => {
                // Assuming correctness on the receiver, there is no
                // good reason why a shutdown would fail and so if it
                // were to we are probably in some really awkward state
                // we shouldn't be in. To be able to recover eventually
                // (by cleaning up), let's at least allow threads using
                // this socket to finish up by indicating that we are
                // shut down indeed, unconditionally.
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
pub struct TcpReceiver<T>
where
    T: Debug + Send,
{
    /// The address we are listening on.
    addr: SocketAddr,
    /// Our listener file descriptor state; shared with the thread
    /// accepting connections.
    fd: Arc<Mutex<Fd>>,
    /// Handle to the thread accepting a connection and processing data.
    thread: Option<JoinHandle<Result<(), String>>>,
    /// The transaction multiplexer we use to ensure serialization of
    /// transactions from all accepted connections.
    txnmux: Arc<Mutex<TxnMux<T, String>>>,
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
        let fd = Arc::new(Mutex::new(Fd::Active(listener.as_raw_fd())));
        let txnmux = Arc::new(Mutex::new(TxnMux::new()));
        let thread = Some(Self::accept(listener, fd.clone(), txnmux.clone()));

        Ok(Self {
            addr,
            fd,
            thread,
            txnmux,
        })
    }

    /// Accept a connection (in a non-blocking manner), read data from
    /// it, and dispatch that to the transaction multiplexer.
    fn accept(
        listener: TcpListener,
        fd: Arc<Mutex<Fd>>,
        txnmux: Arc<Mutex<TxnMux<T, String>>>,
    ) -> JoinHandle<Result<(), String>> {
        spawn(move || {
            let mut handles = Vec::new();
            loop {
                let socket = match listener.accept() {
                    Ok((socket, _)) => socket,
                    Err(e) => {
                        // The user may have dropped the receiver shortly after
                        // us accepting a connection. If that is the case do not
                        // continue.
                        if let Fd::Shutdown = *fd.lock().unwrap() {
                            break;
                        }
                        error!("failed to accept connection: {}", e);
                        continue;
                    }
                };

                let passthrough = Arc::new(Mutex::new(Passthrough::new()));
                let observable = Box::new(passthrough.clone());
                if txnmux.lock().unwrap().add_observable(observable).is_err() {
                    error!(
                        "failed to register connection {} with TxnMux",
                        socket.as_raw_fd()
                    );
                    continue;
                }

                let fd = Arc::new(Mutex::new(Fd::Active(socket.as_raw_fd())));
                let copy = fd.clone();
                let thread = spawn(move || Self::process(socket, copy, passthrough));
                handles.push((thread, fd));
            }

            // We only exit above loop when the receiver is dropped and
            // in this case we intend to stop and join all the
            // processing threads we started.
            for (thread, fd) in handles.into_iter().rev() {
                if let Err(e) = fd.lock().unwrap().shutdown() {
                    error!("failed to shut down TcpReceiver file descriptor: {}", e);
                }
                // It should be safe for us to continue even in case of
                // shutdown failure because all ways in which shutdown
                // can fail are somehow related to an invalid socket
                // (except for EINVAL, but that is trivial to rule out)
                // and so the thread we wait on should have died anyway.
                let _result = thread.join();
                debug_assert!(_result.is_ok(), "processing thread panicked: {:?}", _result);
            }
            Ok(())
        })
    }

    /// Process data from a `TcpSender`, relaying messages to a
    /// connected `Observer`, if any, or dropping them.
    fn process(
        socket: TcpStream,
        fd: Arc<Mutex<Fd>>,
        mut observer: SharedObserver<Passthrough<T, String>>,
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

    /// Retrieve the address we are listening on.
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }
}

impl<T> Drop for TcpReceiver<T>
where
    T: Debug + Send,
{
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
    fn subscribe(
        &mut self,
        observer: ObserverBox<T, String>,
    ) -> Result<Self::Subscription, ObserverBox<T, String>> {
        self.txnmux.lock().unwrap().subscribe(observer)
    }

    /// Unsubscribe a previously subscribed `Observer` based on a
    /// subscription.
    fn unsubscribe(&mut self, subscription: &Self::Subscription) -> Option<ObserverBox<T, String>> {
        self.txnmux.lock().unwrap().unsubscribe(subscription)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::ErrorKind;
    use std::io::Read;
    use std::net::TcpStream;

    use test_env_log::test;

    use crate::await_expected;
    use crate::MockObserver;
    use crate::TcpSender;

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
        let test = || {
            let addr = {
                let recv = TcpReceiver::<()>::new("127.0.0.1:0").unwrap();
                recv.addr().clone()
            };

            TcpStream::connect(addr)
        };

        // There is a teeny-tiny chance that a test running in parallel
        // gets assigned the very same port we were using between the
        // drop of the `TcpReceiver` and the `TcpStream::connect`. To
        // prevent this unlikely collision from causing a flaky test we
        // retry if we did indeed succeed to connect.
        for _ in 1..10 {
            match test() {
                Ok(_) => continue,
                Err(e) => {
                    if e.kind() == ErrorKind::ConnectionRefused {
                        break;
                    } else {
                        panic!("unexpected error")
                    }
                }
            }
        }
    }

    /// Check that senders are properly cleaned up when a `TcpReceiver`
    /// is dropped.
    #[test]
    fn sender_cleanup() {
        let mut send = {
            let recv = TcpReceiver::<()>::new("127.0.0.1:0").unwrap();
            let send = TcpStream::connect(recv.addr()).unwrap();
            send
        };

        let buffer = &mut [0; 32];
        let result = send.read_exact(buffer);
        let error = result.unwrap_err().kind();
        assert!(error == ErrorKind::ConnectionReset || error == ErrorKind::UnexpectedEof);
    }

    /// Test the connection of multiple senders to a `TcpReceiver`.
    #[test]
    fn multiple_senders() {
        let mock = Arc::new(Mutex::new(MockObserver::new()));
        let mut recv = TcpReceiver::<u64>::new("127.0.0.1:0").unwrap();
        let addr = recv.addr();
        let mut send1 = TcpSender::connect(addr).unwrap();
        let mut send2 = TcpSender::connect(addr).unwrap();
        let mut send3 = TcpSender::connect(addr).unwrap();

        recv.subscribe(Box::new(mock.clone())).unwrap();

        let t1 = spawn(move || {
            let send1 = &mut send1 as (&mut dyn Observer<u64, _>);
            let updates1 = vec![1u64];
            send1.on_start().unwrap();
            send1.on_updates(Box::new(updates1.into_iter())).unwrap();
            send1.on_commit().unwrap();
        });

        let t2 = spawn(move || {
            let send2 = &mut send2 as (&mut dyn Observer<u64, _>);
            let updates2 = vec![2u64, 3, 4];
            send2.on_start().unwrap();
            send2.on_updates(Box::new(updates2.into_iter())).unwrap();
            send2.on_commit().unwrap();
        });

        let t3 = spawn(move || {
            let send3 = &mut send3 as (&mut dyn Observer<u64, _>);
            let updates3 = vec![5u64, 6, 7, 8];
            send3.on_start().unwrap();
            send3.on_updates(Box::new(updates3.into_iter())).unwrap();
            send3.on_commit().unwrap();
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();

        await_expected(|| {
            let (on_start, on_updates, on_commit) = {
                let guard = mock.lock().unwrap();
                (
                    guard.called_on_start,
                    guard.called_on_updates,
                    guard.called_on_commit,
                )
            };

            assert_eq!(on_start, 3);
            assert_eq!(on_updates, 8);
            assert_eq!(on_commit, 3);
        });
    }
}
