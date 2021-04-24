use phf::{phf_map, phf_set};
use std::io::{Error, ErrorKind};

#[derive(Clone, Copy, PartialEq)]
#[allow(dead_code)]
enum Lexement {
    Object,
    Array,
    String,
}

#[allow(dead_code)]
static STARTS: phf::Map<char, Lexement> = phf_map! {
    '[' => Lexement::Array,
    '{' => Lexement::Object,
    '"' => Lexement::String,
};

#[allow(dead_code)]
static ENDS: phf::Map<char, Lexement> = phf_map! {
    ']'  => Lexement::Array ,
    '}'  => Lexement::Object,
};

#[allow(dead_code)]
static WHITESPACE: phf::Set<char> = phf_set! {' ', '\t', '\n'};

pub struct JsonFramer {
    w: Vec<Lexement>,
    reassembly: Vec<u8>,
}

impl JsonFramer {
    // certianly backslash - what else?
    //
    // we can avoid utf8 for the framing characters, but we currently dont handle
    // correct reassembly of utf8 across body boundaries
    //
    // dont really _like_ this interface, but ran into borrower problems, in any case
    // we want a stream, right?
    //
    pub fn append(&mut self, body: &[u8]) -> Result<Vec<String>, std::io::Error> {
        let mut n = Vec::new();
        for i in body {
            let c = &(*i as char);

            if match self.w.last() {
                Some(x) => *x == Lexement::String,
                None => false,
            } {
                self.reassembly.push(*i);
                if *c == '"' {
                    self.w.pop(); // backslash
                }
            } else {
                match STARTS.get(c) {
                    Some(x) => self.w.push(*x),
                    None => {
                        // dont allow atoms at the top level
                        if self.w.is_empty() {
                            if !WHITESPACE.contains(c) {
                                return Err(Error::new(
                                    ErrorKind::Other,
                                    format!("extraneaous character {}", c),
                                ));
                            }
                        }
                    }
                }

                if !self.w.is_empty() {
                    self.reassembly.push(*i);
                }

                match ENDS.get(c) {
                    Some(k) => {
                        if *k != self.w.pop().expect("mismatched grouping") {
                            return Err(Error::new(ErrorKind::Other, "mismatched grouping"));
                        }
                        if self.w.is_empty() {
                            let p = match std::str::from_utf8(&self.reassembly[..]) {
                                Ok(x) => x.to_string(),
                                Err(_x) => return Err(Error::new(ErrorKind::Other, "utf8 error")),
                            };
                            n.push(p);
                            self.reassembly.truncate(0);
                        }
                    }
                    None => (),
                }
            }
        }
        Ok(n)
    }

    pub fn new() -> JsonFramer {
        JsonFramer {
            w: Vec::new(),
            reassembly: Vec::new(),
        }
    }
}
