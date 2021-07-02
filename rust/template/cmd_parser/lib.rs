#![warn(missing_debug_implementations)]

mod parse;

use std::fs::{ self, File };
use std::io::{ self, BufRead, BufReader };

pub use parse::*;

use nom::*;
use rustyline::error::ReadlineError;
use rustyline::Editor;

use tokio::sync::{ mpsc, mpsc::error::TrySendError, oneshot };

const HISTORY_FILE: &str = "cmd_parser_history.txt";

// We handle stdin differently depending on whether it is a user terminal or a pipe.
enum Input {
    Tty(Editor<()>),
    Pipe(BufReader<io::Stdin>),
    Source(BufReader<fs::File>),
    TempStore,
}

#[derive(Debug)]
struct CliCmdReq {
    cli_cmd: Vec<u8>,
    resp: Option<oneshot::Sender<Result<bool, String>>>,
}

fn send_cmd_to_parser(tx: &mpsc::Sender<CliCmdReq>, cmd: CliCmdReq) -> Result<bool, String> {
    if let Err(err) = tx.try_send(cmd) {
        match err {
            TrySendError::Full(..) => {
                eprintln!("Parser's input queue is full. Try again later");
                return Ok(false);
            },
            TrySendError::Closed(..) => {
                eprintln!("Receiving side of parser is closed");
                return Err(format!("Receiving side of parser is closed: {}", err));
            },
        }
    }

    Ok(true)
}

/// Parse commands from stdio.
pub async fn interact<F: 'static>(cb: F) -> Result<(), String>
where
    F: Fn(Command, bool) -> (Result<(), String>, bool) + Send,
{
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

    let (tx, mut rx) = mpsc::channel(32);
    let input_thr_handler = tokio::spawn(async move {
        let current_ns: String = String::from("");
        let namespace_stack: Vec<String> = Vec::new();
        let mut sourcing = false;
        let mut original_input = Input::TempStore;
        loop {
            let mut buf: Vec<u8> = Vec::new();
            let line = match &mut input {
                Input::Tty(rl) => {
                    let readline = rl.readline(">> ");
                    match readline {
                        Ok(mut in_line) => {
                            rl.add_history_entry(in_line.as_ref());
                            //println!("Line: {}", line);
                            // If `line` happens to be a comment, it must contain an `\n`, so that the
                            // parser can recognize its end.
                            in_line.push('\n');
                            in_line
                        }
                        Err(ReadlineError::Interrupted) => {
                            println!("CTRL-C");
                            continue;
                        }
                        Err(ReadlineError::Eof) => {
                            println!("CTRL-D");
                            save_history(&rl);
                            let buf: Vec<u8> = Vec::new();
                            let cmd = CliCmdReq {
                                cli_cmd: buf,
                                resp: None,
                            };
                
                            match send_cmd_to_parser(&tx, cmd) {
                                Ok(true) => {},
                                Ok(false) => { panic!("Failed to gracefully terminate parser handler"); },
                                Err(why) => { return Err(why); },
                            }

                            return Ok(());
                        }
                        Err(err) => {
                            save_history(&rl);
                            let buf: Vec<u8> = Vec::new();
                            let cmd = CliCmdReq {
                                cli_cmd: buf,
                                resp: None,
                            };
                
                            match send_cmd_to_parser(&tx, cmd) {
                                Ok(true) => {},
                                Ok(false) => { panic!("Failed to gracefully terminate parser handler"); },
                                Err(why) => { return Err(why); },
                            }

                            return Err(format!("Readline failure: {}", err));
                        }
                    }
                },
                Input::Pipe(reader) => {
                    let mut in_line = String::new();
                    let res = reader.read_line(&mut in_line);
                    match res {
                        Ok(0) => {
                            return Ok(());
                        }
                        Ok(_) => {}
                        Err(err) => {
                            return Err(format!("Failed to read stdin: {}", err));
                        }
                    };
                    in_line
                },
                Input::Source(reader) => {
                    let mut in_line = String::new();
                    let res = reader.read_line(&mut in_line);
                    match res {
                        Ok(0) => {
                            if sourcing {
                                input = original_input;
                                original_input = Input::TempStore;
                                sourcing = false;
                                continue;
                            }
                            else {
                                return Ok(());
                            }
                        }
                        Ok(_) => {}
                        Err(err) => {
                            return Err(format!("Failed to read source file: {}", err));
                        }
                    };
                    in_line
                },
                Input::TempStore => {
                    panic!("Input got into illegal state");
                }
            };

            // Check that input command ends with a semicolon
            match line.find(';') {
                Some(_) => {},
                None => {
                    if line.len() > 1 {
                        eprintln!("Every command should end with ';': {}", line);
                    }
                    
                    continue;
                },
            };

            let mut cmd_tokens = line.split_whitespace();
            let first_cmd = match cmd_tokens.next() {
                Some(cmd) => cmd.replace(";", ""),
                None => { continue; },
            };

            match first_cmd.as_str() {
                // TODO: what to do if source fails? partially executed OK, or do we want complete rollback?
                "source" => {
                    let filename = match cmd_tokens.next() {
                        Some(filename) => filename.replace(";", ""),
                        None => {
                            eprintln!("Please provide source filename");
                            eprintln!("Usage: source <filename>");
                            continue;
                        },
                    };
                    
                    if let Ok(f) = File::open(filename) {
                        sourcing = true;
                        original_input = input;
                        let reader = BufReader::new(f);
                        input = Input::Source(reader);
                    }
                    else {
                        eprintln!("Error during opening source file, please check file name and file path");
                    }

                    continue;
                },
                _ => {},
            }
            // TODO: list of commands/things to add/implement
            /*
            match first_cmd {
                "start" => check if first start or not, and give warning that nested transactions currently not supported
                "commit" => this is to close the current transaction and update the flag
                "exit" => are we in the module or not, and don't forget about nested imports, provide a warning if leaving a module (either EOF or exit) is txn was started but not commited and now it is weird state
                "source" => process import and don't forget about nested imports,
                "insert" => need to modify relation name to include namespace
                "insert_or_update" => ,
                "delete" => ,
                "delete_key" => ,
                "modify" => ,
                "clear",
                "dump",
                "dump rel",
                "fdump", // add this command to write output to file
                "fdump rel", // add this command to write output to file
                "query_index", // not supported by me yet
                "dump_index" // not supported by me yet
                // add support for comma separated commands
            }
            */

            buf.extend_from_slice(line.as_bytes());
            let (resp, rx) = oneshot::channel();
            let cmd = CliCmdReq {
                cli_cmd: buf,
                resp: Some(resp),
            };

            match send_cmd_to_parser(&tx, cmd) {
                Ok(true) => { },
                Ok(false) => { continue; },
                Err(why) => { return Err(why); },
            }

            let resp = rx.await;
            match resp {
                Ok(value) => {
                    match value {
                        Ok(true) => { continue; },
                        Ok(false) => { return Ok(()); },
                        Err(why) => {
                            if namespace_stack.len() > 0 {
                                eprintln!("Encountered an error in module '{}': {}", current_ns, why);
                            }
                            return Err(why);
                        },
                    }
                },
                Err(why) => {
                    if namespace_stack.len() > 0 {
                        eprintln!("Failed to read response from parser in module '{}': {}", current_ns, why);
                        return Err(format!("Failed to read response from parser in module '{}': {}", current_ns, why));
                    }
                },
            }
        }
    });

    let parser_thr_handler = tokio::spawn(async move {
        let interactive = istty;
        loop {
            // Using 'select' instead of simply reading the channel is intentional.
            // Doing so, keeps open an opportunity to add more sources of commands
            // in the future. I'm particularly interested in adding a possibility
            // of connecting remotely to CLI
            let (mut buf, resp) = tokio::select! {
                Some(cmd) = rx.recv() => { (cmd.cli_cmd, cmd.resp) },
                else => {
                    eprintln!("Failed to obtain command to parse: no available command sources");
                    return Err(format!("Failed to obtain command to parse: no available command sources"));
                },
            };
            let resp = match resp {
                Some(resp) => resp,
                None => { return Ok(()); },
            };

            loop {
                let (rest, more) = match parse_command(buf.as_slice()) {
                    Ok((rest, cmd)) => {
                        let (result, cont) = cb(cmd, interactive);
                        if !cont {
                            if let Err(why) = result {
                                if let Err(err) = resp.send(Err(why)) {
                                    eprintln!("Failed to send response back to input handler: {:?}", err);
                                    return Err(format!("Failed to send response back to input handler: {:?}", err));
                                }
                            }
                            else {
                                if let Err(err) = resp.send(Ok(false)) {
                                    eprintln!("Failed to send response back to input handler: {:?}", err);
                                    return Err(format!("Failed to send response back to input handler: {:?}", err));
                                }
                            }
                            return Ok(());
                        }
                        let rest = rest.to_owned();
                        let more = !rest.is_empty();
                        (Some(rest), more)
                    }
                    Err(Err::Incomplete(_)) => (None, false),
                    Err(e) => {
                        let err = format!("Invalid input: {}, ", err_str(&e));
                        if !istty {
                            if let Err(why) = resp.send(Err(err)) {
                                eprintln!("Failed to send response back to input handler: {:?}", why);
                                return Err(format!("Failed to send response back to input handler: {:?}", why));
                            }
                            return Ok(());
                        } else {
                            eprintln!("{}", err);
                        }
                        (Some(Vec::new()), false)
                        //return -1;
                    }
                };
                if let Some(rest) = rest {
                    buf = rest;
                }
                if !more {
                    break;
                }
            }
        }
    });

    let input_out = input_thr_handler.await.unwrap();
    let parser_out = parser_thr_handler.await.unwrap();

    if let Err(why) = parser_out {
        return Err(why);
    }

    fn save_history(rl: &Editor<()>) {
        rl.save_history(HISTORY_FILE).unwrap()
    }

    input_out
}

pub fn err_str<E>(e: &Err<&[u8], E>) -> String {
    match e {
        Err::Error(Context::Code(s, _)) | Err::Failure(Context::Code(s, _)) => {
            String::from_utf8(s.to_vec()).unwrap_or_else(|_| "not a UTF8 string".to_string())
        }
        _ => "".to_string(),
    }
}
