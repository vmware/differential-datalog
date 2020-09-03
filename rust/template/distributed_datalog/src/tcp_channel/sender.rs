use std::fmt::Debug;
use std::io::BufWriter;
use std::io::Error;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::spawn;
use std::thread::JoinHandle;

use log::debug;
use log::error;
use log::trace;
use serde::Serialize;
use uid::Id;

use crate::observe::Observer;
use crate::tcp_channel::socket::Cancelable;
use crate::tcp_channel::socket::Socket;
use crate::tcp_channel::txnbuf::TxnBuf;

/// The sending end of a TCP channel with a specified address and a TCP
/// connection.
#[derive(Debug)]
pub struct TcpSender<T>
where
    T: Debug,
{
    /// The TCP sender's unique ID.
    id: usize,
    /// The buffer we use for buffering transactions or pushing them out
    /// over the wire.
    buffer: Arc<Mutex<TxnBuf<BufWriter<TcpStream>, T>>>,
    /// A cancellation handle we can use for canceling an ongoing
    /// connect.
    cancel: Cancelable,
    /// The thread attempting to establish a connection to a receiver.
    thread: Option<JoinHandle<Result<(), String>>>,
}

impl<T> TcpSender<T>
where
    T: Debug + Send + Serialize + 'static,
{
    /// Create a new `TcpSender`, connecting to the given address.
    pub fn new(addr: SocketAddr) -> Result<Self, Error> {
        let id = Id::<()>::new().get();
        trace!("TcpSender({})::new({})", id, addr);

        let buffer = Arc::new(Mutex::new(TxnBuf::default()));
        let socket = Socket::new()?;
        let cancel = socket.to_cancelable();
        let thread = Some(Self::connect(id, socket, addr, buffer.clone()));

        Ok(Self {
            id,
            buffer,
            cancel,
            thread,
        })
    }

    /// Start a thread attempting to connect to the given address.
    fn connect(
        id: usize,
        socket: Socket,
        addr: SocketAddr,
        buffer: Arc<Mutex<TxnBuf<BufWriter<TcpStream>, T>>>,
    ) -> JoinHandle<Result<(), String>> {
        spawn(move || {
            let stream = socket
                .connect(&addr)
                .map_err(|e| format!("TcpSender({}): failed to connect to {}: {}", id, addr, e))?;
            debug!("TcpSender({}): connected to {}", id, addr);

            let buffer = &mut buffer.lock().unwrap();
            buffer
                .set_mode_passthrough(BufWriter::new(stream))
                .map_err(|e| {
                    format!(
                        "TcpSender({}): failed to flush cached transactions: {}",
                        id, e
                    )
                })?;
            Ok(())
        })
    }
}

impl<T> TcpSender<T>
where
    T: Debug,
{
    /// Block until a connection is established.
    pub fn wait_connected(&mut self) -> Result<(), String> {
        if let Some(t) = self.thread.take() {
            match t.join() {
                Ok(result) => result,
                Err(e) => Err(format!(
                    "TcpSender({}) thread has panicked: {:?}",
                    self.id, e
                )),
            }
        } else {
            Ok(())
        }
    }
}

/// `TcpSender` can be an observer for any type `V` that can be converted to `T`.
/// This way we can support scenarios where `T` is a wrapper that implements the 
/// `Serialize` trait for another type without having to insert an additional
/// transformer in the chain.
impl<T, V> Observer<V, String> for TcpSender<T>
where
    T: Debug + Send + Serialize + From<V> + 'static,
    V: Send,
{
    /// Perform some action before data starts coming in.
    fn on_start(&mut self) -> Result<(), String> {
        trace!("TcpSender({})::on_start", self.id);
        self.buffer.lock().unwrap().on_start()
    }

    /// Send a series of items over the TCP channel.
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = V> + 'a>) -> Result<(), String> {
        trace!("TcpSender({})::on_updates", self.id);
        self.buffer.lock().unwrap().on_updates(Box::new(updates.map(|u| T::from(u))))
    }

    /// Flush the TCP stream and signal the commit.
    fn on_commit(&mut self) -> Result<(), String> {
        trace!("TcpSender({})::on_commit", self.id);
        self.buffer.lock().unwrap().on_commit()
    }

    fn on_completed(&mut self) -> Result<(), String> {
        trace!("TcpSender({})::on_completed", self.id);
        self.buffer.lock().unwrap().on_completed()
    }
}

impl<T> Drop for TcpSender<T>
where
    T: Debug,
{
    fn drop(&mut self) {
        if let Err(e) = self.cancel.cancel() {
            error!("failed to cancel connect: {}", e);
        }
        if let Err(e) = self.wait_connected() {
            error!("{}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use test_env_log::test;

    use crate::await_expected;
    use crate::MockObserver;
    use crate::Observable;
    use crate::SharedObserver;
    use crate::TcpReceiver;

    /// Connect a `TcpSender` to a `TcpReceiver`.
    #[test]
    fn connect() {
        let recv = TcpReceiver::<(),()>::new("127.0.0.1:0").unwrap();
        {
            let _send = TcpSender::<()>::new(*recv.addr());
        }
    }

    /// Transmit updates between a `TcpSender` and a `TcpReceiver`
    /// without an `Observer` being subscribed to the `TcpReceiver`.
    #[test]
    fn transmit_updates_no_consumer() {
        let recv = TcpReceiver::<String, String>::new("127.0.0.1:0").unwrap();
        {
            let mut send = TcpSender::<String>::new(*recv.addr()).unwrap();

            let send = &mut send as &mut dyn Observer<String, _>;
            send.on_start().unwrap();
            send.on_updates(Box::new((1..1000).map(|_| "this-is-a-test".to_string())))
                .unwrap();
            send.on_commit().unwrap();
        }
    }

    #[test]
    fn delayed_connect() {
        let mut send = TcpSender::<u64>::new("127.0.0.1:5006".parse().unwrap()).unwrap();
        let mut recv = TcpReceiver::<u64,u64>::new("127.0.0.1:5006").unwrap();
        let observer = SharedObserver::new(Mutex::new(MockObserver::new()));
        let _ = recv.subscribe(Box::new(observer.clone())).unwrap();

        let send = &mut send as &mut dyn Observer<u64, _>;
        send.on_start().unwrap();
        send.on_updates(Box::new(vec![1, 2, 3].into_iter()))
            .unwrap();
        send.on_commit().unwrap();

        await_expected(|| {
            let on_updates = {
                let mock = observer.lock().unwrap();
                mock.called_on_updates
            };

            assert_eq!(on_updates, 3);
        });
    }
}
