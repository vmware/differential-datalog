use channel::*;

use differential_datalog::program::{RelId, Update, Response};
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};
use api::*;

use std::collections::HashSet;

pub struct UpdatesSubscription<'a> {
    observer: &'a mut Option<Box<dyn Observer<Update<super::Value>, String>>>,
}

impl <'a> Subscription<'a> for UpdatesSubscription<'a> {
    fn unsubscribe(&'a mut self) {
        *self.observer = None;
    }
}

pub struct DDlogServer
{
    prog: HDDlog,
    outlets:  Vec<Outlet>
}

impl DDlogServer
{
    pub fn stream(&mut self, tables: HashSet<RelId>) {
        let outlet = Outlet{tables : tables, observer : None};
        self.outlets.push(outlet);
    }
}

pub struct Outlet
{
    tables: HashSet<RelId>,
    observer: Option<Box<dyn Observer<Update<super::Value>, String>>>
}

impl Observable<Update<super::Value>, String> for Outlet
{
    fn subscribe<'a>(&'a mut self,
                     observer: Box<dyn Observer<Update<super::Value>, String>>)
                     -> Box<dyn Subscription + 'a>
    {
        self.observer = Some(observer);
        Box::new(UpdatesSubscription{ observer: &mut self.observer})
    }
}

impl Observer<Update<super::Value>, String> for DDlogServer
{
    fn on_start(&self) -> Response<()> {
        self.prog.transaction_start()
    }

    fn on_commit(&self) -> Response<()> {
        let changes = self.prog.transaction_commit_dump_changes()?;
        for outlet in &self.outlets {
            if let Some(ref observer) = outlet.observer {
                let upds = outlet.tables.iter().flat_map(|table| {
                    changes.as_ref().get(table).unwrap().iter().map(move |(val, weight)| {
                        debug_assert!(*weight == 1 || *weight == -1);
                        if *weight == 1 {
                            Update::Insert{relid: *table, v: val.clone()}
                        } else {
                            Update::DeleteValue{relid: *table, v: val.clone()}
                        }
                    })
                });

                observer.on_start()?;
                observer.on_updates(Box::new(upds))?;
                observer.on_commit()?;
            }
        };
        Ok(())
    }

    fn on_updates<'a>(&self, updates: Box<dyn Iterator<Item = Update<super::Value>> + 'a>) -> Response<()> {
        self.prog.apply_valupdates(updates)
    }

    fn on_completed(self) -> Response<()> {
        self.prog.stop()
    }
}
