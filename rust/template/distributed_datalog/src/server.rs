use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;

use log::trace;
use uid::Id;

use differential_datalog::ddval::DDValue;
use differential_datalog::program::RelId;
use differential_datalog::program::Update;
use differential_datalog::DDlog;

use crate::observe::Observer;
use crate::observe::ObserverBox;
use crate::observe::OptionalObserver;
use crate::observe::SharedObserver;
use crate::observe::UpdatesObservable;

/// An outlet streams a subset of DDlog tables to an observer.
#[derive(Debug)]
pub struct Outlet {
    tables: BTreeSet<RelId>,
    observer: SharedObserver<OptionalObserver<ObserverBox<Update<DDValue>, String>>>,
}

/// A DDlog server wraps a DDlog program, and writes deltas to the
/// outlets. The redirect map redirects input deltas to local tables.
#[derive(Debug)]
pub struct DDlogServer<P>
where
    P: DDlog,
{
    id: usize,
    created: Instant,
    prog: Option<P>,
    outlets: Vec<Outlet>,
    redirect: HashMap<RelId, RelId>,
}

impl<P> DDlogServer<P>
where
    P: DDlog,
{
    /// Create a new server with no outlets.
    pub fn new(prog: P, redirect: HashMap<RelId, RelId>) -> Self {
        let created = Instant::now();
        let id = Id::<()>::new().get();
        trace!("DDlogServer({})::new", id);

        Self {
            id,
            created,
            prog: Some(prog),
            outlets: Vec::new(),
            redirect,
        }
    }

    /// Add a new outlet that streams a subset of the tables.
    pub fn add_stream(
        &mut self,
        tables: BTreeSet<RelId>,
    ) -> UpdatesObservable<Update<DDValue>, String> {
        trace!("DDlogServer({})::add_stream", self.id);

        let observer = SharedObserver::default();
        let outlet = Outlet {
            tables,
            observer: observer.clone(),
        };
        self.outlets.push(outlet);
        UpdatesObservable { observer }
    }

    /// Remove an outlet.
    pub fn remove_stream(&mut self, stream: UpdatesObservable<Update<DDValue>, String>) {
        trace!("DDlogServer({})::remove_stream", self.id);
        self.outlets
            .retain(|o| !Arc::ptr_eq(&o.observer, &stream.observer));
    }

    /// Shutdown the DDlog program and notify listeners of completion.
    pub fn shutdown(&mut self) -> Result<(), String> {
        trace!("DDlogServer({})::shutdown", self.id);

        // TODO: Right now we may error out early if an observer's
        //       `on_completed` fails. In the future we probably want to push
        //       those errors somewhere and then continue.
        if let Some(mut prog) = self.prog.take() {
            for outlet in &mut self.outlets {
                outlet.observer.on_completed()?
            }
            prog.stop()?;
        }
        Ok(())
    }
}

impl<P> Observer<Update<DDValue>, String> for DDlogServer<P>
where
    P: Debug + Send + DDlog,
{
    /// Start a transaction when deltas start coming in.
    fn on_start(&mut self) -> Result<(), String> {
        trace!("DDlogServer({})::on_start", self.id);

        if let Some(ref mut prog) = self.prog {
            prog.transaction_start()
        } else {
            Ok(())
        }
    }

    /// Commit input deltas to local tables and pass on output deltas to
    /// the listeners on the outlets.
    fn on_commit(&mut self) -> Result<(), String> {
        trace!("DDlogServer({})::on_commit", self.id);

        if let Some(ref mut prog) = self.prog {
            let changes = prog.transaction_commit_dump_changes()?;
            for outlet in &self.outlets {
                let mut observer = outlet.observer.lock().unwrap();
                if observer.is_some() {
                    let mut upds = outlet
                        .tables
                        .iter()
                        .flat_map(|table| {
                            let table = *table;
                            changes.as_ref().get(&table).map(|t| {
                                t.iter().map(move |(val, weight)| {
                                    debug_assert!(*weight == 1 || *weight == -1);
                                    if *weight == 1 {
                                        Update::Insert {
                                            relid: table,
                                            v: val.clone(),
                                        }
                                    } else {
                                        Update::DeleteValue {
                                            relid: table,
                                            v: val.clone(),
                                        }
                                    }
                                })
                            })
                        })
                        .flatten()
                        .peekable();

                    if upds.peek().is_some() {
                        observer.on_start()?;
                        observer.on_updates(Box::new(upds))?;
                        observer.on_commit()?;
                    }
                }
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Apply a series of updates.
    fn on_updates<'a>(
        &mut self,
        updates: Box<dyn Iterator<Item = Update<DDValue>> + 'a>,
    ) -> Result<(), String> {
        trace!("DDlogServer({})::on_updates", self.id);

        if let Some(ref prog) = self.prog {
            prog.apply_valupdates(updates.map(|upd| match upd {
                Update::Insert { relid, v } => Update::Insert {
                    relid: *self.redirect.get(&relid).unwrap_or(&relid),
                    v,
                },
                Update::DeleteValue { relid, v } => Update::DeleteValue {
                    relid: *self.redirect.get(&relid).unwrap_or(&relid),
                    v,
                },
                update => panic!("Operation {:?} not allowed", update),
            }))
        } else {
            trace!("No DDlog program associated with this DDlogServer");
            Ok(())
        }
    }

    fn on_completed(&mut self) -> Result<(), String> {
        trace!("DDlogServer({})::on_completed", self.id);
        println!(
            "DDlogServer received on_completed after {} ms",
            self.created.elapsed().as_millis()
        );
        Ok(())
    }
}

impl<P> Drop for DDlogServer<P>
where
    P: DDlog,
{
    /// Shutdown the DDlog program and notify listeners of completion.
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}
