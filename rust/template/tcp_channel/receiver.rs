use std::fmt::Debug;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::net::SocketAddr;
use std::net::TcpListener;
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
use serde_json::from_str;

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

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::ErrorKind;
    use std::net::TcpStream;

    /// Connect to a `TcpReceiver`.
    #[test]
    fn accept() {
        let recv = TcpReceiver::<()>::new("127.0.0.1:0").unwrap();
        let _ = TcpStream::connect(recv.addr()).unwrap();
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
