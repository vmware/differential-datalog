#![allow(dead_code)]

use crate::{Batch, Error, Evaluator, RecordBatch};
use cmd_parser::{err_str, parse_command, Command};
use differential_datalog::{
    record::RelIdentifier::{RelId, RelName},
    record::UpdCmd,
};
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;

async fn parse_input<F>(filename: String, mut cb: F) -> Result<(), Error>
where
    F: FnMut(Command) -> Result<(), Error>,
{
    let file = tokio::fs::File::open(filename).await?;
    // use the async version
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        println!("lines {}", line);
        match parse_command(line.as_bytes()) {
            Ok((_, cmd)) => cb(cmd)?,
            Err(e) => {
                println!("input error");
                return Err(Error::new(format!("Invalid input: {}, ", err_str(&e))));
            }
        };
    }
    Ok(())
}

pub async fn read_batches_from_file<F>(
    filename: String,
    eval: Evaluator,
    mut cb: F,
) -> Result<(), Error>
where
    F: FnMut(Batch),
{
    let mut b = RecordBatch::new();
    parse_input(filename, |c| -> Result<(), Error> {
        match c {
            Command::Start => Ok(()),
            Command::Update(upd_cmd, _bool) => match upd_cmd {
                UpdCmd::InsertOrUpdate(rel, record) => {
                    let rname = match rel {
                        RelName(name) => name.to_string(),
                        // do we..even want this?
                        RelId(id) => eval.relation_name_from_id(id)?,
                    };
                    b.insert(rname, record, 1);
                    Ok(())
                }
                _ => Err(Error::new("update type unknown".to_string())),
            },
            Command::Commit(_bool) => {
                // shouldn't need to clone here, i'm passing the torch to you cb
                cb(Batch::Rec(b.clone()));
                b = RecordBatch::new();
                Ok(())
            }
            _ => Ok(()),
        }
    })
    .await
}
