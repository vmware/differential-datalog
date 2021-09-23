// this file follows the apparently established pattern of defining a local error type and defining From
// so as to implicity coerce all the errors thrown by '?'
//TODOs:
//  write a macro for the common to_string case below
//  refactor(?) async_error so it doesn't require its users pull in a bunch of dependencies
//  consider adding support for nesting
//  macro for record field extraction case
//  consider adding support for source location if thats possible

use core::convert::Infallible;
use core::fmt;
use core::fmt::Display;
use std::ffi::NulError;
use std::str::Utf8Error;
use tokio::task::JoinError;

#[derive(Debug)]
pub struct Error {
    contents: String,
    line: Option<u64>,
    functionname: Option<String>,
    filename: Option<String>,
    parent: Box<Option<Error>>,
}

#[macro_export]
macro_rules! function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[..name.len() - 3]
    }};
}

impl Error {
    pub fn new(s: String) -> Error {
        Error {
            contents: s,
            line: None,
            functionname: None,
            filename: None,
            parent: Box::new(None),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.contents)
    }
}

// can we really just alpha this ? string is different(?).
impl From<Utf8Error> for Error {
    fn from(err: Utf8Error) -> Self {
        Error::new(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::new(err.to_string())
    }
}

impl From<NulError> for Error {
    fn from(err: NulError) -> Self {
        Error::new(err.to_string())
    }
}

impl From<Infallible> for Error {
    fn from(_err: Infallible) -> Self {
        panic!("can't happen");
    }
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::new(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::new(err.to_string())
    }
}

impl From<JoinError> for Error {
    fn from(err: JoinError) -> Self {
        Error::new(err.to_string())
    }
}

impl From<nix::Error> for Error {
    fn from(err: nix::Error) -> Self {
        Error::new(err.to_string())
    }
}

#[macro_export]
macro_rules! send_error {
    ($e:expr, $t:expr) => {
        $e.error(
            $t.to_string().into_record(),
            std::line!().into_record(),
            std::file!().into_record(),
            function!().into_record(),
        );
    };
}

// wanted to try to wrap this up in a lambda so that the enclosed expr
// could just use ? syntax, but that turns out to be really hard here
#[macro_export]
macro_rules! async_error {
    ($e:expr, $r:expr) => {
        match $r {
            Err(x) => {
                send_error!($e, x);
                return;
            }
            Ok(x) => x,
        }
    };
}

// consider specialized error text
#[macro_export]
macro_rules! async_expect_some {
    ($e:expr, $r:expr) => {
        match $r {
            None => {
                send_error!($e, "expected value");
                return;
            }
            Some(x) => x,
        }
    };
}
