// this module pre-frames json objects out of an arbitrarily chunked byte string.
// it is a really terrible idea to temporarily work around some limtations
// with serde. specifically that it can read chunked blocks and a sequence
// of self-framed json objects, but not both
//
// haven't been able to understand it yet but
// https://github.com/TimelyDataflow/timely-dataflow/blob/master/timely/src/dataflow/operators/to_stream.rs#L73
// may lead to a better answer.
//
// if it were going to stay Lexement should really just be the leading group
// character

use crate::error::Error;
use phf::{phf_map, phf_set};

#[allow(dead_code)]
static STARTS: phf::Set<char> = phf_set! {
    '[',
    '{',
    '"',
};

#[allow(dead_code)]
static ENDS: phf::Map<char, char> = phf_map! {
    ']'  => '[',
    '}'  => '{'
};

#[allow(dead_code)]
static WHITESPACE: phf::Set<char> = phf_set! { ' ', '\t', '\n' };

pub struct JsonFramer {
    w: Vec<char>,
    reassembly: Vec<u8>,
}

impl JsonFramer {
    // certianly backslash - what else?
    //
    // we can avoid utf8 for the framing characters, but we currently dont handle
    // correct reassembly of utf8 across body boundaries
    //
    // dont really _like_ this interface, but ran into borrower problems, in any case
    // we want a stream, right? lambda over finishished results seems to work fine
    //
    pub fn append(&mut self, body: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        let mut n = Vec::new();
        for i in body {
            let c = *i as char;

            if match self.w.last() {
                Some(x) => *x == '"',
                None => false,
            } {
                self.reassembly.push(*i);
                if c == '"' {
                    self.w.pop(); // backslash
                }
            } else {
                if STARTS.contains(&c) {
                    self.w.push(c);
                } else {
                    // dont allow atoms at the top level
                    if self.w.is_empty() && !WHITESPACE.contains(&c) {
                        return Err(Error::new(format!(
                            "extraneaous character {}",
                            std::str::from_utf8(body).expect("")
                        )));
                    }
                }

                if !self.w.is_empty() {
                    self.reassembly.push(*i);
                }

                if let Some(k) = ENDS.get(&c) {
                    if *k != self.w.pop().expect("mismatched grouping") {
                        return Err(Error::new("mismatched grouping".to_string()));
                    }
                    if self.w.is_empty() {
                        n.push(self.reassembly[..].to_vec());
                        self.reassembly.truncate(0);
                    }
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
