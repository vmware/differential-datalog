extern crate nom;
extern crate num;
extern crate rustyline;
extern crate libc;

extern crate differential_datalog;

mod parse;

pub use parse::*;

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
pub fn interact<F>(cb: F) -> i32 
    where F: Fn(Command) -> (i32, bool)
{
    let mut buf: Vec<u8> = Vec::new(); 

    let mut input = if unsafe { libc::isatty(libc::STDIN_FILENO as i32) } != 0 {
        let mut rl = Editor::<()>::new();
        let _ = rl.load_history(HISTORY_FILE);
        Input::TTY(rl)
    } else {
        Input::Pipe(BufReader::new(io::stdin()))
    };

    let istty = match &input {
        Input::TTY(_) => true,
        _             => false
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
                    Ok(0)  => {
                        return 0;
                    },
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
                    let (status, cont) = cb(cmd);
                    if !cont {
                        return status;
                    };
                    (Some(rest.to_owned()), true)
                },
                Err(Err::Incomplete(_)) => { (None, false) },
                Err(e) => {
                    eprintln!("Invalid input: {}, ", err_str(&e));
                    if !istty {
                        return -1;
                    };
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

fn err_str<E>(e: &Err<&[u8], E>) -> String {
    match e {
        Err::Error(Context::Code(s,_))    => String::from_utf8(s.to_vec()).unwrap_or("not a UTF8 string".to_string()),
        Err::Failure(Context::Code(s, _)) => String::from_utf8(s.to_vec()).unwrap_or("not a UTF8 string".to_string()),
        _ => "".to_string()
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
