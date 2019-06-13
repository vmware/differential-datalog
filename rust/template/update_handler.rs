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
use std::sync::{Arc, Barrier, Mutex, MutexGuard};
use std::cell::RefCell;
use super::valmap::*;

/* Single-threaded (non-thread-safe callback)
 */
pub trait ST_CBFn<V>: Fn(RelId, &V, bool) {
    fn clone_boxed(&self) -> Box<dyn ST_CBFn<V>>;
}

impl<T, V> ST_CBFn<V> for T
where
    V: Val,
    T: 'static + Clone + Fn(RelId, &V, bool)
{
    fn clone_boxed(&self) -> Box<dyn ST_CBFn<V>> {
        Box::new(self.clone())
    }
}

impl <V: Val> Clone for Box<dyn ST_CBFn<V>> {
    fn clone(&self) -> Self {
        self.as_ref().clone_boxed()
    }
}

pub trait UpdateHandler<V> {
    /* Returns a handler to be invoked on each output relation update. */
    fn update_cb(&self) -> Box<dyn ST_CBFn<V>>;

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
pub trait MTUpdateHandler<V>: UpdateHandler<V> + Sync + Send {
    /* Returns a thread-safe handler to be invoked on each output
     * relation update. */
    fn mt_update_cb(&self) -> Box<dyn CBFn<V>>;
}

/* Rust magic to make `MTUpdateHandler` clonable.
 */
pub trait IMTUpdateHandler<V>: MTUpdateHandler<V> {
   fn clone_boxed(&self) -> Box<dyn IMTUpdateHandler<V>>;
}

impl<T, V> IMTUpdateHandler<V> for T
where
   T: MTUpdateHandler<V> + Clone + 'static
{
   fn clone_boxed(&self) -> Box<dyn IMTUpdateHandler<V>> {
       Box::new(self.clone())
   }
}

impl<V> Clone for Box<dyn IMTUpdateHandler<V>> {
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
    fn update_cb(&self) -> Box<dyn ST_CBFn<V>> {
        Box::new(|_,_,_|{})
    }
    fn before_commit(&self) {}
    fn after_commit(&self, _success: bool) {}
}

impl <V: Val> MTUpdateHandler<V> for NullUpdateHandler
{
    fn mt_update_cb(&self) -> Box<dyn CBFn<V>> {
        Box::new(|_,_,_|{})
    }
}

/* `UpdateHandler` implementation that invokes user-provided callback.
 */
#[derive(Clone)]
pub struct ExternCUpdateHandler {
    cb:      ExternCCallback,
    cb_arg:  libc::uintptr_t
}

type ExternCCallback = extern "C" fn(arg: libc::uintptr_t,
                                     table: libc::size_t,
                                     rec: *const record::Record,
                                     polarity: bool);

impl ExternCUpdateHandler
{
    pub fn new(cb: ExternCCallback, cb_arg: libc::uintptr_t) -> Self {
        Self{cb, cb_arg}
    }
}

impl <V: Val + IntoRecord> UpdateHandler<V> for ExternCUpdateHandler
{
    fn update_cb(&self) -> Box<dyn ST_CBFn<V>> {
        let cb = self.cb.clone();
        let cb_arg = self.cb_arg.clone();
        Box::new(move |relid, v, w|cb(cb_arg, relid, &v.clone().into_record() as *const record::Record, w))
    }
    fn before_commit(&self) {}
    fn after_commit(&self, _success: bool) {}
}

impl <V: Val + IntoRecord> MTUpdateHandler<V> for ExternCUpdateHandler
{
    fn mt_update_cb(&self) -> Box<dyn CBFn<V>> {
        let cb = self.cb.clone();
        let cb_arg = self.cb_arg.clone();
        Box::new(move |relid, v, w|cb(cb_arg, relid, &v.clone().into_record() as *const record::Record, w))
    }
}

/* Multi-threaded `UpdateHandler` implementation that stores updates
 * in a `ValMap` and locks the map on every update.
 */
#[derive(Clone)]
pub struct MTValMapUpdateHandler {
    db: Arc<Mutex<valmap::ValMap>>
}

impl MTValMapUpdateHandler
{
    pub fn new(db: Arc<Mutex<valmap::ValMap>>) -> Self {
        Self{db}
    }
}

impl UpdateHandler<Value> for MTValMapUpdateHandler
{
    fn update_cb(&self) -> Box<dyn ST_CBFn<Value>> {
        let db = self.db.clone();
        Box::new(move |relid, v, w|db.lock().unwrap().update(relid, v, w))
    }
    fn before_commit(&self) {}
    fn after_commit(&self, _success: bool) {}
}

impl MTUpdateHandler<Value> for MTValMapUpdateHandler
{
    fn mt_update_cb(&self) -> Box<dyn CBFn<Value>> {
        let db = self.db.clone();
        Box::new(move |relid, v, w|db.lock().unwrap().update(relid, v, w))
    }
}

/* Single-threaded `UpdateHandler` implementation that stores updates
 * in a `ValMap`, locking the map for the entire duration of a commit.
 * After the commit is done, the map can be accessed from a different
 * thread.
 */
#[derive(Clone)]
pub struct ValMapUpdateHandler {
    db: Arc<Mutex<ValMap>>,
    /* Stores pointer to `MutexGuard` between `before_commit()` and
    * `after_commit()`.  This has to be unsafe, because Rust does
    * not let us express a borrow from a field of the same struct. */
    locked: Arc<RefCell<*mut libc::c_void>>
}

impl Drop for ValMapUpdateHandler {
    /* Release the mutex if still held. */
    fn drop<'a>(&'a mut self) {
        let guard_ptr = self.locked.replace(ptr::null_mut()) as *mut MutexGuard<'a, ValMap>;
        if guard_ptr != ptr::null_mut() {
            let _guard: Box<MutexGuard<'_, ValMap>> = unsafe { Box::from_raw(guard_ptr) };
        }
    }
}

impl ValMapUpdateHandler
{
    pub fn new(db: Arc<Mutex<ValMap>>) -> Self {
        Self{db, locked: Arc::new(RefCell::new(ptr::null_mut()))}
    }
}

impl UpdateHandler<Value> for ValMapUpdateHandler
{
    fn update_cb(&self) -> Box<dyn ST_CBFn<Value>> {
        let handler = self.clone();
        Box::new(move |relid, v, w|{
            let guard_ptr = handler.locked.borrow();
            /* `update_cb` can only be called between `before_commit()` and
             * `after_commit()` */
            assert_ne!(*guard_ptr, ptr::null_mut());
            let mut guard: Box<MutexGuard<'_, ValMap>> = unsafe {
                Box::from_raw(*guard_ptr as *mut MutexGuard<'_, ValMap>)
            };
            guard.update(relid, v, w);
            Box::into_raw(guard);
        })
    }
    fn before_commit(&self) {
        let guard = Box::into_raw(Box::new(self.db.lock().unwrap())) as *mut libc::c_void;
        let old = self.locked.replace(guard);
        assert_eq!(old, ptr::null_mut());
    }
    fn after_commit(&self, _success: bool) {
        let guard_ptr = self.locked.replace(ptr::null_mut());
        assert_ne!(guard_ptr, ptr::null_mut());
        let _guard = unsafe {
            Box::from_raw(guard_ptr as *mut MutexGuard<'_, ValMap>)
        };
        /* Lock will be released when `_guard` goes out of scope. */
    }
}


/* `UpdateHandler` implementation that chains multiple single-threaded
 * handlers.
 */
pub struct ChainedUpdateHandler<V: Val> {
    handlers: Vec<Box<dyn UpdateHandler<V>>>
}

impl <V: Val> ChainedUpdateHandler<V> {
    pub fn new(handlers: Vec<Box<dyn UpdateHandler<V>>>) -> Self {
        Self{ handlers }
    }
}

impl <V: Val> UpdateHandler<V> for ChainedUpdateHandler<V>
{
    fn update_cb(&self) -> Box<dyn ST_CBFn<V>> {
        let cbs: Vec<Box<dyn ST_CBFn<V>>> = self.handlers.iter().map(|h|h.update_cb()).collect();
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

/* `UpdateHandler` implementation that chains multiple multi-threaded
 * handlers.
 */
#[derive(Clone)]
pub struct MTChainedUpdateHandler<V: Val> {
    handlers: Vec<Box<dyn IMTUpdateHandler<V>>>
}

impl <V: Val> MTChainedUpdateHandler<V> {
    pub fn new(handlers: Vec<Box<dyn IMTUpdateHandler<V>>>) -> Self {
        Self{ handlers }
    }
}

impl <V: Val> UpdateHandler<V> for MTChainedUpdateHandler<V>
{
    fn update_cb(&self) -> Box<dyn ST_CBFn<V>> {
        let cbs: Vec<Box<dyn ST_CBFn<V>>> = self.handlers.iter().map(|h|h.update_cb()).collect();
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

impl <V: Val> MTUpdateHandler<V> for MTChainedUpdateHandler<V>
{
    fn mt_update_cb(&self) -> Box<dyn CBFn<V>> {
        let cbs: Vec<Box<dyn CBFn<V>>> = self.handlers.iter().map(|h|h.mt_update_cb()).collect();
        Box::new(move |relid, v, w| {
            for cb in cbs.iter() {
                cb(relid, v, w);
            }
        })
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
    msg_channel: Arc<Mutex<SyncSender<Msg<V>>>>,

    /* Barrier to synchronize completion of transaction with worker. */
    commit_barrier: Arc<Barrier>
}

impl <V: Val> ThreadUpdateHandler<V>
{
    pub fn new<F>(handler_generator: F, queue_capacity: usize) -> Self 
        where F: FnOnce() -> Box<dyn UpdateHandler<V>> + Send + 'static
    {
        let (tx_msg_channel, rx_message_channel) = sync_channel(queue_capacity);
        let commit_barrier = Arc::new(Barrier::new(2));
        let commit_barrier2 = commit_barrier.clone();
        spawn(move || {
            let handler = handler_generator();
            let update_cb = handler.update_cb();
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
    fn update_cb(&self) -> Box<dyn ST_CBFn<V>> {
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

impl <V: Val + IntoRecord> MTUpdateHandler<V> for ThreadUpdateHandler<V>
{
    fn mt_update_cb(&self) -> Box<dyn CBFn<V>> {
        let channel = self.msg_channel.lock().unwrap().clone();
        Box::new(move |relid, v, w| {
            channel.send(Msg::Update{relid, v: v.clone(), w});
        })
    }
}
