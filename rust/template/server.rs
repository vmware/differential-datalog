use fallible_iterator::FallibleIterator;
use channel::*;
use differential_datalog::program::{RelId, Update};
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use api::*;
use std::iter;

pub struct DDlogServer<O>
where O: Observer<Update<super::Value>, String>
{
    prog: HDDlog,
    observer: O
}

pub struct OutLet;

impl <O> Observer<Update<super::Value>, String> for DDlogServer<O>
where O: Observer<Update<super::Value>, String>
{
    fn on_start(&self) {
        self.prog.transaction_start();
    }

    fn on_commit(&self) {
        // TODO handle errors
        let changes = self.prog.transaction_commit_dump_changes();
        if let Ok(changes) = changes {
            let upds = changes.as_ref().iter().flat_map(|(table_id, table_data)| {
                table_data.iter().map(move |(val, weight)| {
                    debug_assert!(*weight == 1 || *weight == -1);
                    if *weight == 1 {
                        Update::Insert{
                            relid: *table_id,
                            v: val.clone()
                        }
                    } else {
                        Update::DeleteValue{
                            relid: *table_id,
                            v: val.clone()
                        }
                    }
                })
            });

            self.observer.on_start();
            self.observer.on_updates(upds);
            self.observer.on_commit();
        }
    }

    fn on_updates(&self, updates: impl Iterator<Item = Update<super::Value>>) {
        self.prog.apply_valupdates(updates);
    }

    fn on_completed(self) {
        self.prog.stop();
    }
}

impl <O> Observable<Update<super::Value>, String, O> for DDlogServer<O>
where O: Observer<Update<super::Value>, String>
{
    fn subscribe(&mut self, observer: O) {
        self.observer = observer;
    }
}
