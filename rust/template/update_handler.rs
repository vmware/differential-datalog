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
use std::sync::{Arc, Barrier, Mutex};

pub trait UpdateHandler<V>: Send {
    /* Returns a handler to be invoked on each output relation update. */
    fn update(&self) -> Box<dyn CBFn<V>>;

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
    fn update(&self) -> Box<dyn CBFn<V>> {
        Box::new(|_,_,_|{})
    }
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
    fn update(&self) -> Box<dyn CBFn<V>> {
        let cb = self.cb.clone();
        let cb_arg = self.cb_arg.clone();
        Box::new(move |relid, v, w|cb(cb_arg, relid, &v.clone().into_record() as *const record::Record, w))
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
    fn update(&self) -> Box<dyn CBFn<Value>> {
        let db = self.db.clone();
        Box::new(move |relid, v, w|db.lock().unwrap().update(relid, v, w))
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
        Self{ handlers }
    }
}

impl <V: Val> UpdateHandler<V> for ChainedDynUpdateHandler<V>
{
    fn update(&self) -> Box<dyn CBFn<V>> {
        let cbs: Vec<Box<dyn CBFn<V>>> = self.handlers.iter().map(|h|h.update()).collect();
        Box::new(move |relid, v, w| {
            for cb in cbs.iter() {
                cb(relid, v, w);
            }
        })
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

/* `UpdateHandler` implementation that handles updates in a separate
 * worker thread.
 */

/* We use a single mpsc channel to notify worker about
 * update, start, and commit events. */
enum Msg<V: Val> {
    BeforeCommit,
    Update{relid: RelId, v: V, w: bool},
    AfterCommit{success: bool},
    Stop
}

#[derive(Clone)]
pub struct ThreadUpdateHandler<V: Val> {
    /* Channel to worker thread. */
    msg_channel: Arc<Mutex<Sender<Msg<V>>>>,

    /* Barrier to synchronize completion of transaction with worker. */
    commit_barrier: Arc<Barrier>
}

impl <V: Val> ThreadUpdateHandler<V>
{
    pub fn new<H: UpdateHandler<V> + 'static>(handler: H) -> Self {
        let (tx_msg_channel, rx_message_channel) = channel();
        let commit_barrier = Arc::new(Barrier::new(2));
        let commit_barrier2 = commit_barrier.clone();
        spawn(move || {
            let update_cb = handler.update();
            loop {
                match rx_message_channel.recv() {
                    Ok(Msg::Update{relid, v, w}) => {
                        update_cb(relid, &v, w);
                    },
                    Ok(Msg::BeforeCommit) => {
                        handler.before_commit();
                    },
                    Ok(Msg::AfterCommit{success}) => {
                        /* All updates have been sent to channel by now: flush the channel. */
                        loop {
                            match rx_message_channel.try_recv() {
                                Ok(Msg::Update{relid, v, w}) => {
                                    update_cb(relid, &v, w);
                                },
                                Ok(Msg::Stop) => { return; },
                                _ => { break; }
                            }
                        };
                        handler.after_commit(success);
                        commit_barrier2.wait();
                    },
                    Ok(Msg::Stop) => { return; },
                    _ => { return; }
                }
            }
        });
        Self {
            msg_channel: Arc::new(Mutex::new(tx_msg_channel)),
            commit_barrier: commit_barrier
        }
    }
}

impl<V: Val> Drop for ThreadUpdateHandler<V> {
    fn drop(&mut self) {
        self.msg_channel.lock().unwrap().send(Msg::Stop);
    }
}

impl <V: Val + IntoRecord> UpdateHandler<V> for ThreadUpdateHandler<V>
{
    fn update(&self) -> Box<dyn CBFn<V>> {
        let channel = self.msg_channel.lock().unwrap().clone();
        Box::new(move |relid, v, w| {
            channel.send(Msg::Update{relid, v: v.clone(), w});
        })
    }
    fn before_commit(&self) {
        self.msg_channel.lock().unwrap().send(Msg::BeforeCommit);
    }
    fn after_commit(&self, success: bool) {
        self.msg_channel.lock().unwrap().send(Msg::AfterCommit{success});

        /* Wait for all queued updates to get processed by worker. */
        self.commit_barrier.wait();
    }
}
