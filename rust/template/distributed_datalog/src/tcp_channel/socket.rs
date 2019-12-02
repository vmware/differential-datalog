//! A module providing an asynchronously cancelable socket. Rust's
//! `std::net::TcpStream` does not allow for interrupting a connection
//! attempt midway, as there is simply no API for doing so. As a clumsy
//! work around one could use `std::net::TcpStream::connect_timeout`
//! with a short timeout, after which one could check for a cancellation
//! request, but that requires an unnecessary amount of cycles.
//!
//! This module provides a way to cancel a connection request issued
//! earlier by separating socket creation from connection.

use std::convert::TryFrom;
use std::convert::TryInto;
use std::io::Error;
use std::io::ErrorKind;
use std::mem::forget;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::FromRawFd;
use std::os::unix::io::IntoRawFd;
use std::os::unix::io::RawFd;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use log::error;

trait IsMinusOne {
    fn is_minus_one(&self) -> bool;
}

macro_rules! impl_is_minus_one {
    ($($t:ident)*) => ($(impl IsMinusOne for $t {
        fn is_minus_one(&self) -> bool {
            *self == -1
        }
    })*)
}

impl_is_minus_one! { i8 i16 i32 i64 isize }

fn cvt<T: IsMinusOne>(t: T) -> Result<T, Error> {
    if t.is_minus_one() {
        Err(Error::last_os_error())
    } else {
        Ok(t)
    }
}

fn into_inner(addr: &SocketAddr) -> (*const libc::sockaddr, libc::socklen_t) {
    match addr {
        SocketAddr::V4(ref a) => (
            a as *const _ as *const _,
            std::mem::size_of_val(a) as libc::socklen_t,
        ),
        SocketAddr::V6(ref a) => (
            a as *const _ as *const _,
            std::mem::size_of_val(a) as libc::socklen_t,
        ),
    }
}

fn set_nonblocking(fd: RawFd, nonblocking: bool) -> Result<(), Error> {
    let mut nonblocking = nonblocking as libc::c_int;
    cvt(unsafe { libc::ioctl(fd, libc::FIONBIO, &mut nonblocking) }).map(|_| ())
}

/// Attempt to connect the given socket file descriptor.
///
/// This function will not return until either
/// 1) the socket was connected successfully
/// OR
/// 2) `closable` was closed asynchronously
// This function is derived from the standard library's `connect_timeout`,
// but without the timeout part, and instead added support for
// asynchronous cancellation.
fn connect(socket: &Fd, addr: &SocketAddr) -> Result<(), Error> {
    'connect: loop {
        let fd = socket.as_raw_fd();
        set_nonblocking(fd, true)?;
        let r = unsafe {
            let (addrp, len) = into_inner(addr);
            cvt(libc::connect(fd, addrp, len))
        };
        set_nonblocking(fd, false)?;

        match r {
            Ok(_) => return Ok(()),
            Err(e) => {
                let os_err = e.raw_os_error();
                if os_err == Some(libc::ECONNREFUSED) {
                    continue 'connect;
                }
                if os_err != Some(libc::EINPROGRESS) {
                    return Err(e);
                }
            }
        }

        let mut pollfds = [libc::pollfd {
            fd,
            events: libc::POLLOUT,
            revents: 0,
        }];

        // We use an infinite timeout. Users can cancel the operation,
        // though.
        let timeout = -1;
        let count = pollfds.len().try_into().unwrap();

        loop {
            match unsafe { libc::poll(pollfds.as_mut_ptr(), count, timeout) } {
                -1 => {
                    let err = Error::last_os_error();
                    if err.kind() != ErrorKind::Interrupted {
                        return Err(err);
                    }
                }
                0 => {
                    // This case shouldn't really happen as it only applies
                    // to timeouts. Ultimately it's just like any other
                    // spurious wakeup and we continue.
                }
                _ => {
                    if socket.is_shutdown() {
                        return Err(Error::from_raw_os_error(libc::ENOTCONN));
                    }
                    // Linux returns POLLOUT|POLLERR|POLLHUP for refused
                    // connections (!), so look for POLLHUP rather than read
                    // readiness. If the connection got refused we start
                    // over attempting to connect again.
                    if pollfds[0].revents & libc::POLLHUP != 0 {
                        // TODO: We could probably do better than busy
                        //       waiting, but it is not clear how.
                        //       Perhaps we just sleep or we have some
                        //       form of exponential backoff on repeated
                        //       connection failures?
                        continue 'connect;
                    }
                    return Ok(());
                }
            }
        }
    }
}

/// A trait providing shutdown functionality to certain objects.
pub trait ShutdownExt {
    /// Shut down the object.
    ///
    /// Note that no matter whether the shutdown actually succeeds or
    /// not, the object won't ever allow another shutdown but always
    /// return `Ok`.
    fn shutdown(&self) -> Result<(), Error>;

    /// Check whether the object has been shut down.
    fn is_shutdown(&self) -> bool;
}

const FD_CLOSED: usize = 1 << (0usize.count_zeros() - 1);
const FD_SHUTDOWN: usize = FD_CLOSED >> 1;
const FD_UNOWNED: usize = FD_SHUTDOWN >> 1;

#[derive(Debug)]
pub struct Fd(AtomicUsize);

impl Fd {
    /// Create a new `Fd` object that owns the given file descriptor,
    /// i.e., it will be closed when the object is dropped.
    pub fn new<T>(fd: T) -> Self
    where
        // We deliberately use c_uint here which pushes the burden of
        // checking for negative values to the client (while still
        // having covering the full range of valid values).
        T: Into<libc::c_uint>,
    {
        debug_assert_eq!(FD_CLOSED.count_ones(), 1);
        debug_assert_eq!(FD_SHUTDOWN.count_ones(), 1);
        debug_assert_eq!(FD_UNOWNED.count_ones(), 1);

        let fd = usize::try_from(fd.into()).unwrap();
        assert_eq!(fd & FD_SHUTDOWN, 0);
        assert_eq!(fd & FD_CLOSED, 0);
        assert_eq!(fd & FD_UNOWNED, 0);

        // This type is based on the assumption that sizeof(c_uint) >=
        // sizeof(usize), which really should hold all platforms we are
        // interested in.
        Fd(fd.into())
    }

    /// Create an `Fd` object without transferring ownership.
    ///
    /// The wrapped file descriptor will not be closed when the
    /// resulting object is dropped.
    pub fn new_unowned<T>(fd: T) -> Self
    where
        T: Into<libc::c_uint>,
    {
        let s = Self::new(fd);
        let _ = s.0.fetch_or(FD_UNOWNED, Ordering::SeqCst);
        s
    }

    /// Safely close the file descriptor.
    ///
    /// Note that no matter whether the close actually succeeds or not,
    /// the object won't ever allow another close.
    pub fn close(&self) -> Result<(), Error> {
        let fd = self.0.fetch_or(FD_CLOSED, Ordering::SeqCst);
        // Note that we don't care about the shut down state of the
        // object in this method, as shutting down the file descriptor
        // does not invalidate it.
        if fd & (FD_CLOSED | FD_UNOWNED) != 0 {
            Ok(())
        } else {
            let fd = fd & !FD_SHUTDOWN;
            cvt(unsafe { libc::close(fd.try_into().unwrap()) }).map(|_| ())
        }
    }

    /// Check whether the object has been closed.
    pub fn is_closed(&self) -> bool {
        self.0.load(Ordering::SeqCst) & FD_CLOSED != 0
    }
}

impl ShutdownExt for Fd {
    fn shutdown(&self) -> Result<(), Error> {
        let fd = self.0.fetch_or(FD_SHUTDOWN, Ordering::SeqCst);
        if fd & (FD_SHUTDOWN | FD_CLOSED) != 0 {
            Ok(())
        } else {
            let fd = fd & !FD_UNOWNED;
            cvt(unsafe { libc::shutdown(fd.try_into().unwrap(), libc::SHUT_RDWR) }).map(|_| ())
        }
    }

    fn is_shutdown(&self) -> bool {
        self.0.load(Ordering::SeqCst) & FD_SHUTDOWN != 0
    }
}

impl AsRawFd for Fd {
    fn as_raw_fd(&self) -> RawFd {
        let fd = self.0.load(Ordering::SeqCst) & !(FD_SHUTDOWN | FD_CLOSED | FD_UNOWNED);
        // It's always safe to unwrap because we created the object from
        // what is essentially a RawFd to begin with, it's just that we
        // store it in something potentially larger.
        fd.try_into().unwrap()
    }
}

impl IntoRawFd for Fd {
    fn into_raw_fd(self) -> RawFd {
        let fd = self.as_raw_fd();
        forget(self);
        fd
    }
}

impl Drop for Fd {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            error!(
                "failed to close file descriptor ({}): {}",
                self.0.load(Ordering::SeqCst),
                e
            );
        }
    }
}

/// Create a socket file descriptor.
fn socket() -> Result<Fd, Error> {
    let fd =
        cvt(unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) })?;
    Ok(Fd::new(fd as libc::c_uint))
}

/// An object representing a socket.
#[derive(Debug)]
pub struct Socket(Arc<Fd>);

impl Socket {
    /// Create a new `Socket` object.
    pub fn new() -> Result<Self, Error> {
        Ok(Self(Arc::new(socket()?)))
    }

    /// Connect the socket to the given address.
    ///
    /// This function will not return until a connection has been
    /// established or the socket been closed.
    pub fn connect(self, addr: &SocketAddr) -> Result<TcpStream, Error> {
        connect(&self.0, addr).map(|_| {
            // It's always safe to unwrap here because we only ever
            // handed out weak references.
            let fd = Arc::try_unwrap(self.0).unwrap();
            unsafe { TcpStream::from_raw_fd(fd.into_raw_fd()) }
        })
    }

    /// Retrieve a reference to a file descriptor that can be used to
    /// asynchronously close the socket, unblocking any in-progress
    /// connect(2) operations.
    pub fn to_cancelable(&self) -> Cancelable {
        Cancelable(Arc::downgrade(&self.0))
    }
}

#[derive(Debug)]
pub struct Cancelable(Weak<Fd>);

impl Cancelable {
    /// Cancel the cancelable.
    pub fn cancel(&self) -> Result<(), Error> {
        if let Some(fd) = self.0.upgrade() {
            match fd.shutdown() {
                // Don't signal an error if we just haven't connected yet.
                Err(ref e) if e.raw_os_error() == Some(libc::ENOTCONN) => Ok(()),
                r => r,
            }
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Read;
    use std::io::Write;
    use std::net::TcpListener;
    use std::thread::sleep;
    use std::thread::spawn;
    use std::time::Duration;

    /// Test the closing on an `Fd`.
    #[test]
    fn close() {
        let socket = Socket::new().unwrap();
        let fd = socket.0;
        let raw = fd.as_raw_fd();

        assert!(!fd.is_closed());
        assert!(fd.close().is_ok());
        assert!(fd.is_closed());
        assert_eq!(fd.as_raw_fd(), raw);

        assert!(fd.close().is_ok());
        assert!(fd.is_closed());
        assert_eq!(fd.as_raw_fd(), raw);
    }

    /// Check the shutdown of an `Fd` and the invariants that go with
    /// that.
    #[test]
    fn shutdown() {
        // In order to properly test the shut down functionality we
        // first have to connect the socket.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let fd = socket().unwrap();
        connect(&fd, &addr).unwrap();

        assert!(!fd.is_shutdown());
        assert!(fd.shutdown().is_ok());
        assert!(fd.is_shutdown());

        assert!(fd.shutdown().is_ok());
        assert!(fd.is_shutdown());

        assert!(fd.close().is_ok());
        assert!(fd.is_shutdown());
    }

    /// Test that we can establish a connection.
    #[test]
    fn connect_immediately() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let socket = Socket::new().unwrap();
        let _ = socket.connect(&addr).unwrap();
    }

    /// Check that we can eventually connect when a service starts
    /// listening on an address.
    #[test]
    fn connect_eventually() {
        const MESSAGE: &[u8] = b"success\0";
        const ADDR: &str = "127.0.0.1:5003";

        fn test(delay_ns: u64) {
            let thread = spawn(move || {
                if delay_ns > 0 {
                    sleep(Duration::from_nanos(delay_ns));
                }

                let listener = TcpListener::bind(ADDR).unwrap();
                // Wait until we got an incoming connection and then exit
                // the thread.
                let (mut socket, _) = listener.accept().unwrap();
                let mut data = Vec::new();
                let _ = socket.read_to_end(&mut data).unwrap();
                assert_eq!(data.as_slice(), MESSAGE);
            });

            {
                let sock_addr = ADDR.parse().unwrap();
                let socket = Socket::new().unwrap();
                let mut stream = socket.connect(&sock_addr).unwrap();
                let _ = stream.write_all(&MESSAGE).unwrap();
            }

            thread.join().unwrap();
        }

        for delay in (0..100_000).step_by(1_000) {
            test(delay);
        }
    }

    /// Test cancellation of an attempted connect when there is no
    /// entity to connect to.
    #[test]
    fn cancel_no_accept() {
        const ADDR: &str = "127.0.0.1:5004";

        let sock_addr = ADDR.parse().unwrap();
        let socket = Socket::new().unwrap();
        let cancelable = socket.to_cancelable();

        let thread = spawn(move || {
            let err = socket.connect(&sock_addr).unwrap_err();
            assert_eq!(err.raw_os_error(), Some(libc::ENOTCONN));
        });

        let _ = cancelable.cancel().unwrap();

        thread.join().unwrap();
    }

    /// Test cancellation anywhere between attempting to connect and the
    /// connection being accepted.
    #[test]
    fn cancel_with_bind() {
        const ADDR: &str = "127.0.0.1:5005";

        let sock_addr = ADDR.parse().unwrap();
        let socket = Socket::new().unwrap();
        let cancelable = socket.to_cancelable();

        let thread1 = spawn(move || {
            let _listener = TcpListener::bind(ADDR).unwrap();
            // We probably don't want to actually accept the connection,
            // because we don't know when the cancellation happens. If
            // it happens before we connect, the accept would just block
            // forever.
            sleep(Duration::from_micros(100))
        });

        let thread2 = spawn(move || {
            let _stream = socket.connect(&sock_addr);
        });

        // The operation may or may not succeed, depending on whether we
        // actually connect or not.
        let _result = cancelable.cancel();

        // The main thing we care about is that the connect exits after
        // cancelation, which is guaranteed to have happened after the
        // join succeeded.
        thread1.join().unwrap();
        thread2.join().unwrap();
    }
}
