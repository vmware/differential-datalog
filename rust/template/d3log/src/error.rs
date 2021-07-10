// this file follows the apparently established pattern of defining a local error type and defining From
// so as to implicity coerce all the errors thrown by '?'

use core::fmt;
use core::fmt::Display;
use std::ffi::NulError;
use std::str::Utf8Error;
use tokio::task::JoinError;

#[derive(Debug)]
pub struct Error(String);

//TODOs:
//  write a macro for the common to_string case below
//  write a user macro to enforce option be Some
//  refactor(?) async_error so it doesn't require its users pull in a bunch of dependencies
//  consider adding support for nesting
//  consider adding support for source location if thats possible

impl Error {
    pub fn new(s: String) -> Error {
        Error(s)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
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

// wanted to try to wrap this up in a lambda so that the enclosed expr
// could just use ? syntax, but that turns out to be really hard here
#[macro_export]
macro_rules! async_error {
    ($p:expr, $r:expr) => {
        match $r {
            Err(x) => {
                let s = x.to_string();
                println!("async error: {}", s);
                $p.send(fact!(d3::Error, text => s.into_record()));
                return;
            }
            Ok(x) => x,
        }
    }
}
