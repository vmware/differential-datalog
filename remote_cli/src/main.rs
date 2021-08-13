use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use std::io::{self, Read};
use tokio::io as other_io;
use rustyline::Editor;

#[tokio::main]
async fn main() -> io::Result<()> {
    let socket = TcpStream::connect("127.0.0.1:9876").await?;
    println!("connected!");
    let (mut rd, mut wr) = other_io::split(socket);

    println!("socket is split");
    // Write data in the background
    let write_task = tokio::spawn(async move {
        let mut rl = Editor::<()>::new();
        loop {
            println!(">> ");
            let mut buffer = String::new();
//            io::stdin().read_to_string(&mut buffer).unwrap();

//            let line = format!("{}\r\n", buffer);
            let readline = rl.readline(">> ");
            let line = match readline {
                Ok(mut in_line) => {
                    in_line.push('\n');
                    in_line
                },
                Err(_) => { panic!("panic!"); },
            };
            println!("got line: {}", line);
            let mut buf: Vec<u8> = Vec::new();
            buf.extend_from_slice(line.as_bytes());
        wr.write(&buf).await.unwrap();

        // Sometimes, the rust type inferencer needs
        // a little help
        }
        //Ok::<_, io::Error>(())
    });

    println!("time to await...");
    let _out = write_task.await;
    println!("done");

    Ok(())
}
