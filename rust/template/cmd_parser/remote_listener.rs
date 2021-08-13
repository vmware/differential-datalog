#![warn(missing_debug_implementations)]

use std::fs::{ self, File };
use std::io::{ self, BufRead, BufReader };

use nom::*;
use rustyline::error::ReadlineError;
use rustyline::Editor;

use tokio::sync::{ mpsc, mpsc::error::TrySendError, oneshot };
use tokio::sync::mpsc::Sender;
use tokio::io as other_io;
use tokio::net::TcpListener;

use crate::CliCmdReq;

// We handle stdin differently depending on whether it is a user terminal or a pipe.
enum Input {
    Tty(Editor<()>),
    Pipe(BufReader<io::Stdin>),
    Source(BufReader<fs::File>),
    TempStore,
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
pub async fn remote_listen(tx: Sender<CliCmdReq>) -> Result<(), String>
{
    let listener = TcpListener::bind("127.0.0.1:9876").await.unwrap();
    let input_thr_handler = tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok((stream, x)) => (stream, x),
                Err(why) => { return Err(format!("{}", why)); },
            };
            // let mut in_buf = vec![0; 256];
            let mut buf: Vec<u8> = Vec::new();
            other_io::copy(&mut stream, &mut buf);

            
            // buf.extend_from_slice(line.as_bytes());
            let (resp, rx) = oneshot::channel();
            let cmd = CliCmdReq {
                cli_cmd: buf,
                resp: Some(resp),
            };

            match send_cmd_to_parser(&tx, cmd) {
                Ok(true) => { },
                Ok(false) => { continue; },
                Err(why) => { return Err(format!("{}", why)); },
            }

            let resp = rx.await;
            match resp {
                Ok(value) => {
                    match value {
                        Ok(true) => { continue; },
                        Ok(false) => { return Ok(()); },
                        Err(why) => {
                            return Err(format!("{}", why));
                        },
                    }
                },
                Err(why) => {
                    return Err(format!("Failed to read response from parser: {}", why));
                },
            }
        }
    });

    Ok(())
}