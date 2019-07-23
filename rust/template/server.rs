use differential_datalog::program::{RelId, Update, Response};
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};

use api::*;
use channel::*;

use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};
use std::fmt::{Debug, Formatter};
use std::fmt;

pub struct UpdatesSubscription<'a> {
    observer: &'a mut Option<Arc<Mutex<dyn Observer<Update<super::Value>, String>>>>,
}

impl <'a> UpdatesSubscription<'a> {
    pub fn new(observer: &'a mut Option<Arc<Mutex<dyn Observer<Update<super::Value>, String>>>>) -> Self {
        UpdatesSubscription{observer: observer}
    }
}

impl <'a> Subscription<'a> for UpdatesSubscription<'a> {
    fn unsubscribe(&'a mut self) {
        *self.observer = None;
    }
}

pub struct DDlogServer
{
    prog: HDDlog,
    outlets: Vec<Outlet>,
    redirect: HashMap<RelId, RelId>
}

impl Debug for DDlogServer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DDlogServer")
    }
}

impl DDlogServer
{
    pub fn new(prog: HDDlog, redirect: HashMap<RelId, RelId>) -> Self {
        DDlogServer{prog: prog, outlets: Vec::new(), redirect: redirect}
    }

    pub fn stream(&mut self, tables: HashSet<RelId>) -> &mut Outlet {
        let outlet = Outlet{tables : tables, observer : None};
        self.outlets.push(outlet);
        self.outlets.last_mut().unwrap()
    }
}

pub struct Outlet
{
    tables: HashSet<RelId>,
    observer: Option<Arc<Mutex<dyn Observer<Update<super::Value>, String>>>>
}

impl Observable<Update<super::Value>, String> for Outlet
{
    fn subscribe<'a>(&'a mut self,
                     observer: Arc<Mutex<dyn Observer<Update<super::Value>, String>>>)
                     -> Box<dyn Subscription + 'a>
    {
        self.observer = Some(observer);
        Box::new(UpdatesSubscription::new(&mut self.observer))
    }
}

impl Observer<Update<super::Value>, String> for DDlogServer
{
    fn on_start(&mut self) -> Response<()> {
        self.prog.transaction_start()
    }

    fn on_commit(&mut self) -> Response<()> {
        let changes = self.prog.transaction_commit_dump_changes()?;
        for change in changes.as_ref().iter() {
            println!{"Got {:?}", change};
        }
        for outlet in &mut self.outlets {
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

                let mut observer = observer.lock().unwrap();
                observer.on_start()?;
                observer.on_updates(Box::new(upds))?;
                observer.on_commit()?;
            }
        };
        Ok(())
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<super::Value>> + 'a>) -> Response<()> {
        self.prog.apply_valupdates(updates.map(|upd| match upd {
            Update::Insert{relid: relid, v: v} =>
                Update::Insert{
                    relid: *self.redirect.get(&relid).unwrap(),
                    v: v},
            Update::DeleteValue{relid: relid, v: v} =>
                Update::DeleteValue{
                    relid: *self.redirect.get(&relid).unwrap(),
                    v: v},
            Update::DeleteKey{relid: relid, k: k} =>
                Update::DeleteKey{
                    relid: *self.redirect.get(&relid).unwrap(),
                    k: k},
            Update::Modify{relid: relid, k: k, m: m} =>
                Update::Modify{
                    relid: *self.redirect.get(&relid).unwrap(),
                    k: k,
                    m: m},
        }))
    }

    fn on_completed(self) -> Response<()> {
        self.prog.stop()
    }
}
