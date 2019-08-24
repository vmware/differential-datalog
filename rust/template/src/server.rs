use differential_datalog::program::{RelId, Update, Response};
use differential_datalog::record::{Record, UpdCmd, RelIdentifier};

use api::*;
use observe::*;

use std::collections::{HashSet, HashMap};
use std::sync::{Arc, Mutex};
use std::fmt::{Debug, Formatter};
use std::fmt;

pub struct UpdatesSubscription {
    // This points to the observer field in the outlet,
    // and sets it to `None` upon unsubscribing
    observer: Arc<Mutex<Option<Box<dyn Observer<Update<super::Value>, String>>>>>
}

impl Subscription for UpdatesSubscription {
    // Cancel a subscription so that the observer stops listening
    // to the observable
    fn unsubscribe(self: Box<Self>) {
        let mut observer = self.observer.lock().unwrap();
        *observer = None;
    }
}

// A DDlog server wraps a DDlog progam, and writes deltas to
// the outlets. The redirect map redirects input deltas to
// local tables
pub struct DDlogServer
{
    prog: Option<HDDlog>,
    outlets: Vec<Arc<Mutex<Outlet>>>,
    redirect: HashMap<RelId, RelId>
}

impl DDlogServer
{
    // Create a new server with no outlets
    pub fn new(prog: HDDlog, redirect: HashMap<RelId, RelId>) -> Self {
        DDlogServer{prog: Some(prog), outlets: Vec::new(), redirect: redirect}
    }

    // Add a new outlet that streams a subset of the tables
    pub fn add_stream(&mut self, tables: HashSet<RelId>) -> Arc<Mutex<Outlet>> {
        let outlet = Arc::new(Mutex::new(Outlet{
            tables : tables,
            observer : Arc::new(Mutex::new(None))
        }));
        self.outlets.push(outlet.clone());
        outlet.clone()
    }

    // Remove an outlet
    pub fn remove_stream(&mut self, outlet: Arc<Mutex<Outlet>>) {
        self.outlets.retain(|o| !Arc::ptr_eq(&o, &outlet));
    }

    // Shutdown the DDlog program and notify listeners of completion
    pub fn shutdown(&mut self) -> Response<()> {
        if let Some(prog) = self.prog.take() {
            prog.stop()?;
        }
        for outlet in &self.outlets {
            let outlet = outlet.lock().unwrap();
            let observer = outlet.observer.clone();
            let mut observer = observer.lock().unwrap();
            if let Some(ref mut observer) = *observer {
                observer.on_completed()?;
            }
        };
        Ok(())
    }
}

// An outlet streams a subset of DDlog tables to an observer
pub struct Outlet
{
    tables: HashSet<RelId>,
    observer: Arc<Mutex<Option<Box<dyn Observer<Update<super::Value>, String>>>>>
}

impl Observable<Update<super::Value>, String> for Outlet
{
    // An observer subscribes to the delta stream from an outlet
    fn subscribe(&mut self,
                     observer: Box<dyn Observer<Update<super::Value>, String> + Send>)
                     -> Box<dyn Subscription>
    {
        let obs = self.observer.clone();
        let mut obs = obs.lock().unwrap();
        *obs = Some(observer);
        Box::new(UpdatesSubscription{
            observer: self.observer.clone()
        })
    }
}

// Newtype to for interior mutability of the observer
pub struct ADDlogServer(pub Arc<Mutex<DDlogServer>>);

impl Observer<Update<super::Value>, String> for ADDlogServer {
    fn on_start(&mut self) -> Response<()> {
        let s = self.0.clone();
        println!("trying to lock");
        let mut s = s.lock().unwrap();
        println!("locked");
        s.on_start()
    }

    fn on_commit(&mut self) -> Response<()> {
        let s = self.0.clone();
        let mut s = s.lock().unwrap();
        s.on_commit()
    }

    fn on_next(&mut self, upd: Update<super::Value>) -> Response<()> {
        let s = self.0.clone();
        let mut s = s.lock().unwrap();
        s.on_next(upd)
    }

    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<super::Value>> + 'a>) -> Response<()> {
        let s = self.0.clone();
        let mut s = s.lock().unwrap();
        s.on_updates(updates)
    }

    fn on_error(&self, error: String) {
        let s = self.0.clone();
        let s = s.lock().unwrap();
        s.on_error(error)
    }

    fn on_completed(&mut self) -> Response<()> {
        let s = self.0.clone();
        let mut s = s.lock().unwrap();
        s.on_completed()
    }
}

impl Observer<Update<super::Value>, String> for DDlogServer
{
    // Start a transaction when deltas start coming in
    fn on_start(&mut self) -> Response<()> {
        if let Some(ref mut prog) = self.prog {
            prog.transaction_start()
        } else {
            Ok(())
        }
    }

    // Commit input deltas to local tables and pass on output
    // deltas to the listeners on the outlets
    fn on_commit(&mut self) -> Response<()> {
        if let Some(ref mut prog) = self.prog {
            let changes = prog.transaction_commit_dump_changes()?;
            for change in changes.as_ref().iter() {
                println!{"Got {:?}", change};
            }
            for outlet in &self.outlets {
                let outlet = outlet.clone();
                let outlet = outlet.lock().unwrap();
                let observer = outlet.observer.clone();
                let mut observer = observer.lock().unwrap();
                if let Some(ref mut observer) = *observer {
                    let upds = outlet.tables.iter().flat_map(|table| {
                        changes.as_ref().get(table).map(|t| {
                            t.iter().map(move |(val, weight)| {
                                debug_assert!(*weight == 1 || *weight == -1);
                                if *weight == 1 {
                                    Update::Insert{relid: *table, v: val.clone()}
                                } else {
                                    Update::DeleteValue{relid: *table, v: val.clone()}
                                }
                            })
                        })
                    }).flatten();

                    observer.on_start()?;
                    observer.on_updates(Box::new(upds))?;
                    observer.on_commit()?;
                }
            };
            Ok(())
        } else {
            Ok(())
        }
    }

    // Apply a single update
    fn on_next(&mut self, upd: Update<super::Value>) -> Response<()> {
        let upd = vec![match upd {
            Update::Insert{relid: relid, v: v} =>
                Update::Insert{
                    relid: *self.redirect.get(&relid).unwrap_or(&relid),
                    v: v},
            Update::DeleteValue{relid: relid, v: v} =>
                Update::DeleteValue{
                    relid: *self.redirect.get(&relid).unwrap_or(&relid),
                    v: v},
            _otherwise => panic!("Operation not allowed"),
        }];
        if let Some(ref mut prog) = self.prog {
            prog.apply_valupdates(upd.into_iter())
        } else {
            Ok(())
        }
    }

    // Apply a series of updates
    fn on_updates<'a>(&mut self, updates: Box<dyn Iterator<Item = Update<super::Value>> + 'a>) -> Response<()> {
        if let Some(ref prog) = self.prog {
            prog.apply_valupdates(updates.map(|upd| match upd {
                Update::Insert{relid: relid, v: v} =>
                    Update::Insert{
                        relid: *self.redirect.get(&relid).unwrap_or(&relid),
                        v: v},
                Update::DeleteValue{relid: relid, v: v} =>
                    Update::DeleteValue{
                        relid: *self.redirect.get(&relid).unwrap_or(&relid),
                        v: v},
                _otherwise => panic!("Operation not allowed"),
            }))
        } else {
            Ok(())
        }
    }

    fn on_error(&self, error: String) {
        println!("error: {:?}", error);
    }

    fn on_completed(&mut self) -> Response<()> {
        Ok(())
    }
}
