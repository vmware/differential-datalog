//! `UpdateHandler` trait and its implementations.
//!
//! `UpdateHandler` abstracts away different methods of handling
//! relation update notifications from differential dataflow.
//! Possible implementations include:
//! - handling notifications by invoking a user-defined callback
//! - storing output tables in an in-memory database
//! - accumulating changes from one or multiple transactions in
//!   an in-memory database
//! - chaining multiple update handlers
//! - all of the above, but processed by a separate thread
//!   rather than the differential worker threads that computes
//!   the update
//! - all of the above, but processed by a pool of worker threads

use super::*;

use std::thread::*;
use std::sync::mpsc::*;

pub trait UpdateHandler<V>: Send {
    /* Invoked on each output relation update */
    fn update(&self, relid: RelId, v: &V, w: bool);

    /* Notifies the handler that a transaction_commit method is about to be
     * called. The handler has an opportunity to prepare to handle
     * update notifications. */
    fn before_commit(&self);

    /* Notifies the handler that transaction_commit has finished.  The
     * `success` flag indicates whether the commit succeeded or failed. */
    fn after_commit(&self, success: bool);
}

/* Multi-threaded update handler that can be invoked from multiple DDlog
 * worker threads.
 */
pub trait MTUpdateHandler<V>: UpdateHandler<V> + Sync {}
impl <V: Val, H: UpdateHandler<V> + Sync + Send> MTUpdateHandler<V> for H {}

type ExternCCallback = extern "C" fn(arg: libc::uintptr_t,
                                     table: libc::size_t,
                                     rec: *const record::Record,
                                     polarity: bool);

/* Rust magic to make `MTUpdateHandler` clonable.
 */
pub trait DynUpdateHandler<V>: MTUpdateHandler<V> {
   fn clone_boxed(&self) -> Box<dyn DynUpdateHandler<V>>;
}

impl<T, V> DynUpdateHandler<V> for T
where
   T: MTUpdateHandler<V> + Clone + 'static
{
   fn clone_boxed(&self) -> Box<dyn DynUpdateHandler<V>> {
       Box::new(self.clone())
   }
}

impl<V> Clone for Box<dyn DynUpdateHandler<V>> {
   fn clone(&self) -> Self {
       self.as_ref().clone_boxed()
   }
}

/* A no-op `UpdateHandler` implementation
 */
#[derive(Clone)]
pub struct NullUpdateHandler {}

impl NullUpdateHandler
{
    pub fn new() -> Self {
        Self{}
    }
}

impl <V: Val> UpdateHandler<V> for NullUpdateHandler
{
    fn update(&self, _relid: RelId, _v: &V, _w: bool) {}
    fn before_commit(&self) {}
    fn after_commit(&self, _success: bool) {}
}

/* `UpdateHandler` implementation that invokes user-provided callback.
 */
#[derive(Clone)]
pub struct ExternCUpdateHandler {
    cb:      ExternCCallback,
    cb_arg:  libc::uintptr_t
}

impl ExternCUpdateHandler
{
    pub fn new(cb: ExternCCallback, cb_arg: libc::uintptr_t) -> Self {
        Self{cb, cb_arg}
    }
}

impl <V: Val + IntoRecord> UpdateHandler<V> for ExternCUpdateHandler
{
    fn update(&self, relid: RelId, v: &V, w: bool) {
        (self.cb)(self.cb_arg, relid, &v.clone().into_record() as *const record::Record, w)
    }
    fn before_commit(&self) {}
    fn after_commit(&self, success: bool) {}
}

/* Multi-threaded `UpdateHandler` implementation that stores updates
 * in a `ValMap` and locks the map on every update.
 */
#[derive(Clone)]
pub struct MTValMapUpdateHandler {
    db: sync::Arc<sync::Mutex<valmap::ValMap>>
}

impl MTValMapUpdateHandler
{
    pub fn new(db: sync::Arc<sync::Mutex<valmap::ValMap>>) -> Self {
        Self{db}
    }
}

impl UpdateHandler<Value> for MTValMapUpdateHandler
{
    fn update(&self, relid: RelId, v: &Value, w: bool) {
        self.db.lock().unwrap().update(relid, v, w)
    }
    fn before_commit(&self) {}
    fn after_commit(&self, success: bool) {}
}

/* `UpdateHandler` implementation that chains multiple multi-threaded
 * handlers.
 */
#[derive(Clone)]
pub struct ChainedDynUpdateHandler<V: Val> {
    handlers: Vec<Box<dyn DynUpdateHandler<V>>>
}

impl <V: Val> ChainedDynUpdateHandler<V> {
    pub fn new(handlers: Vec<Box<dyn DynUpdateHandler<V>>>) -> Self {
        Self{handlers}
    }
}

impl <V: Val> UpdateHandler<V> for ChainedDynUpdateHandler<V>
{
    fn update(&self, relid: RelId, v: &V, w: bool) {
        for h in self.handlers.iter() {
            h.update(relid, v, w);
        }
    }
    fn before_commit(&self) {
        for h in self.handlers.iter() {
            h.before_commit();
        }
    }

    fn after_commit(&self, success: bool) {
        for h in self.handlers.iter() {
            h.after_commit(success);
        }
    }
}
