#[macro_use]
extern crate nom;
extern crate num;
extern crate rustyline;
extern crate libc;

mod parse;
mod from_value;

pub use parse::*;
pub use from_value::*;

use nom::*;
use std::io;
use std::io::{BufReader, BufRead};
use rustyline::error::ReadlineError;
use rustyline::Editor;

const HISTORY_FILE: &str = "cmd_parser_history.txt";

// We handle stdin differently depending on whether it is a user terminal or a pipe.
enum Input {
    TTY(Editor<()>),
    Pipe(BufReader<io::Stdin>)
}

/// Parse commands from stdio.
pub fn interact(cb: fn(Command)) -> i32 {
    let mut buf: Vec<u8> = Vec::new(); 

    let mut input = if unsafe { libc::isatty(libc::STDIN_FILENO as i32) } != 0 {
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
                    Ok(line) => {
                        rl.add_history_entry(line.as_ref());
                        //println!("Line: {}", line);
                        line
                    },
                    Err(ReadlineError::Interrupted) => {
                        println!("CTRL-C");
                        continue;
                    },
                    Err(ReadlineError::Eof) => {
                        println!("CTRL-D");
                        save_history(&rl);
                        return 0;
                    },
                    Err(err) => {
                        println!("Error: {:?}", err);
                        save_history(&rl);
                        return -1;
                    }
                }
            },
            Input::Pipe(reader) => {
                let mut line = String::new();
                let res = reader.read_line(&mut line);
                match res {
                    Ok(_)  => {},
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
            let (rest, more) = match parse_command(buf.as_slice()) {
                Ok((rest, cmd)) => {
                    cb(cmd);
                    (Some(rest.to_owned()), true)
                },
                Err(Err::Incomplete(_)) => { (None, false) },
                Err(e) => {
                    eprintln!("Invalid input: {}", e);
                    (Some(Vec::new()), false)
                    //return -1;
                }
            };
            match rest {
                Some(rest) => { buf = rest },
                _ => {}
            };
            if !more {break;}
        }
    };

    fn save_history(rl: &Editor<()>) {
        rl.save_history(HISTORY_FILE).unwrap()
    }
}

// uncomment to test command line interaction

/*
#[cfg(test)]
fn echo_cb(cmd: Command) {
    eprintln!("Command: {:?}", cmd)
}

#[test]
fn test_interactive() {
    let res = interact(echo_cb);
    assert_eq!(res, 0);
}
*/
