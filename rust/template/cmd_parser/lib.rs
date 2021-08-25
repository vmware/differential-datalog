#![warn(missing_debug_implementations)]

mod parse;

use std::io;
use std::io::BufRead;
use std::io::BufReader;

pub use parse::*;

use nom::*;
use rustyline::error::ReadlineError;
use rustyline::Editor;

const HISTORY_FILE: &str = "cmd_parser_history.txt";

// We handle stdin differently depending on whether it is a user terminal or a pipe.
enum Input {
    Tty(Editor<()>),
    Pipe(BufReader<io::Stdin>),
}

/// Parse commands from stdio.
pub fn interact<F>(cb: F) -> Result<(), String>
where
    F: Fn(Command, bool) -> (Result<(), String>, bool),
{
    let mut buf: Vec<u8> = Vec::new();

    let istty = unsafe {
        // libc::STDIN_FILENO
        libc::isatty(0)
    } != 0;
    let mut input = if istty {
        let mut rl = Editor::<()>::new();
        let _ = rl.load_history(HISTORY_FILE);
        Input::Tty(rl)
    } else {
        Input::Pipe(BufReader::new(io::stdin()))
    };

    loop {
        let line = match &mut input {
            Input::Tty(rl) => {
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
                        save_history(rl);
                        return Ok(());
                    }
                    Err(err) => {
                        save_history(rl);
                        return Err(format!("Readline failure: {}", err));
                    }
                }
            }
            Input::Pipe(reader) => {
                let mut line = String::new();
                let res = reader.read_line(&mut line);
                match res {
                    Ok(0) => {
                        return Ok(());
                    }
                    Ok(_) => {}
                    Err(err) => {
                        return Err(format!("Failed to read stdin: {}", err));
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
                    let (result, cont) = cb(cmd, interactive);
                    if !cont {
                        return result;
                    };
                    let rest = rest.to_owned();
                    let more = !rest.is_empty();
                    (Some(rest), more)
                }
                Err(Err::Incomplete(_)) => (None, false),
                Err(e) => {
                    let err = format!("Invalid input: {}, ", err_str(&e));
                    if !istty {
                        return Err(err);
                    } else {
                        eprintln!("{}", err);
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

pub fn err_str<E>(e: &Err<&[u8], E>) -> String {
    match e {
        Err::Error(Context::Code(s, _)) | Err::Failure(Context::Code(s, _)) => {
            String::from_utf8(s.to_vec()).unwrap_or_else(|_| "not a UTF8 string".to_string())
        }
        _ => "".to_string(),
    }
}
