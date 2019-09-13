#![allow(clippy::char_lit_as_u8)]

extern crate libc;
extern crate nom;
extern crate num;
extern crate rustyline;

extern crate differential_datalog;

mod parse;

pub use parse::*;

use nom::*;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::io;
use std::io::{BufRead, BufReader};

const HISTORY_FILE: &str = "cmd_parser_history.txt";

// We handle stdin differently depending on whether it is a user terminal or a pipe.
enum Input {
    TTY(Editor<()>),
    Pipe(BufReader<io::Stdin>),
}

/// Parse commands from stdio.
pub fn interact<F>(cb: F) -> i32
where
    F: Fn(Command, bool) -> (i32, bool),
{
    let mut buf: Vec<u8> = Vec::new();

    let istty = unsafe { libc::isatty(libc::STDIN_FILENO as i32) } != 0;
    let mut input = if istty {
        let mut rl = Editor::<()>::new();
        let _ = rl.load_history(HISTORY_FILE);
        Input::TTY(rl)
    } else {
        Input::Pipe(BufReader::new(io::stdin()))
    };

    loop {
        let line = match &mut input {
            Input::TTY(rl) => {
                let readline = rl.readline(">> ");
                match readline {
                    Ok(mut line) => {
                        rl.add_history_entry(line.as_ref());
                        //println!("Line: {}", line);
                        // If `line` happens to be a comment, it must contain an `\n`, so that the
                        // parser can recognize its end.
                        line.push('\n');
                        line
                    }
                    Err(ReadlineError::Interrupted) => {
                        println!("CTRL-C");
                        continue;
                    }
                    Err(ReadlineError::Eof) => {
                        println!("CTRL-D");
                        save_history(&rl);
                        return 0;
                    }
                    Err(err) => {
                        println!("Error: {:?}", err);
                        save_history(&rl);
                        return -1;
                    }
                }
            }
            Input::Pipe(reader) => {
                let mut line = String::new();
                let res = reader.read_line(&mut line);
                match res {
                    Ok(0) => {
                        return 0;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Failed to read stdin: {}", e);
                        return -1;
                    }
                };
                line
            }
        };

        buf.extend_from_slice(line.as_bytes());

        loop {
            let interactive = istty;
            let (rest, more) = match parse_command(buf.as_slice()) {
                Ok((rest, cmd)) => {
                    let (status, cont) = cb(cmd, interactive);
                    if !cont {
                        return status;
                    };
                    let rest = rest.to_owned();
                    let more = !rest.is_empty();
                    (Some(rest), more)
                }
                Err(Err::Incomplete(_)) => (None, false),
                Err(e) => {
                    eprintln!("Invalid input: {}, ", err_str(&e));
                    if !istty {
                        return -1;
                    };
                    (Some(Vec::new()), false)
                    //return -1;
                }
            };
            if let Some(rest) = rest {
                buf = rest
            };
            if !more {
                break;
            }
        }
    }

    fn save_history(rl: &Editor<()>) {
        rl.save_history(HISTORY_FILE).unwrap()
    }
}

fn err_str<E>(e: &Err<&[u8], E>) -> String {
    match e {
        Err::Error(Context::Code(s, _)) | Err::Failure(Context::Code(s, _)) => {
            String::from_utf8(s.to_vec()).unwrap_or_else(|_| "not a UTF8 string".to_string())
        }
        _ => "".to_string(),
    }
}
