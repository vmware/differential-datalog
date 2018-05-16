#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_shorthand_field_patterns)]
#![allow(unused_variables)]
extern crate timely;
extern crate timely_communication;
#[macro_use]
extern crate abomonation;
extern crate differential_dataflow;
extern crate num;

use num::bigint::BigUint;
use abomonation::Abomonation;

#[macro_use] 
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
use std::ops::*;
use serde::ser::*;
use serde::de::*;
use std::str::FromStr;
use serde::de::Error;
use std::collections::HashSet;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::io::{stdin, stdout, Write};
use std::cell::RefCell;
use std::rc::Rc;
use std::hash::Hash;
use serde_json as json;

use timely::progress::nested::product::Product;
use timely::dataflow::*;
use timely::dataflow::scopes::{Child, Root};
use timely::dataflow::operators::*;
use timely::dataflow::operators::feedback::Handle;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::progress::timestamp::RootTimestamp;

use timely_communication::Allocator;


use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::{Data, Collection, Hashable};
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

/// A collection defined by multiple mutually recursive rules.
///
/// A `Variable` names a collection that may be used in mutually recursive rules. This implementation
/// is like the `Variable` defined in `iterate.rs` optimized for Datalog rules: it supports repeated
/// addition of collections, and a final `distinct` operator applied before connecting the definition.
pub struct Variable<'a, G: Scope, D: Default+Data+Hashable>
where G::Timestamp: Lattice+Ord {
    feedback: Option<Handle<G::Timestamp, u64,(D, Product<G::Timestamp, u64>, isize)>>,
    current: Collection<Child<'a, G, u64>, D>,
    cycle: Collection<Child<'a, G, u64>, D>,
}

impl<'a, G: Scope, D: Default+Data+Hashable> Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    /// Creates a new `Variable` from a supplied `source` stream.
    pub fn from(source: &Collection<Child<'a, G, u64>, D>) -> Variable<'a, G, D> {
        let (feedback, cycle) = source.inner.scope().loop_variable(u64::max_value(), 1);
        let cycle = Collection::new(cycle);
        let mut result = Variable { feedback: Some(feedback), current: cycle.clone(), cycle: cycle };
        result.add(source);
        result
    }
    /// Adds a new source of data to the `Variable`.
    pub fn add(&mut self, source: &Collection<Child<'a, G, u64>, D>) {
        self.current = self.current.concat(source);
    }
}

impl<'a, G: Scope, D: Default+Data+Hashable> ::std::ops::Deref for Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    type Target = Collection<Child<'a, G, u64>, D>;
    fn deref(&self) -> &Self::Target {
        &self.cycle
    }
}

impl<'a, G: Scope, D: Default+Data+Hashable> Drop for Variable<'a, G, D> where G::Timestamp: Lattice+Ord {
    fn drop(&mut self) {
        if let Some(feedback) = self.feedback.take() {
            self.current.distinct()
                        .inner
                        .connect_loop(feedback);
        }
    }
}

#[derive(Eq, PartialOrd, PartialEq, Ord, Debug, Clone, Hash)]
struct Uint{x:BigUint}

impl Default for Uint {
    fn default() -> Uint {Uint{x: BigUint::default()}}
}
unsafe_abomonate!(Uint);

impl Serialize for Uint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.serialize_str(&self.x.to_str_radix(10))
    }
}

impl<'de> Deserialize<'de> for Uint {
    fn deserialize<D>(deserializer: D) -> Result<Uint, D::Error>
        where D: Deserializer<'de>
    {
        match String::deserialize(deserializer) {
            Ok(s) => match BigUint::from_str(&s) {
                        Ok(i)  => Ok(Uint{x:i}),
                        Err(_) => Err(D::Error::custom(format!("invalid integer value: {}", s)))
                     },
            Err(e) => Err(e)
        }
    }
}

impl Uint {
    #[inline]
    pub fn parse_bytes(buf: &[u8], radix: u32) -> Uint {
        Uint{x: BigUint::parse_bytes(buf, radix).unwrap()}
    }
}

impl Shr<usize> for Uint {
    type Output = Uint;

    #[inline]
    fn shr(self, rhs: usize) -> Uint {
        Uint{x: self.x.shr(rhs)}
    }
}

impl Shl<usize> for Uint {
    type Output = Uint;

    #[inline]
    fn shl(self, rhs: usize) -> Uint {
        Uint{x: self.x.shl(rhs)}
    }
}

macro_rules! forward_binop {
    (impl $imp:ident for $res:ty, $method:ident) => {
        impl $imp<$res> for $res {
            type Output = $res;

            #[inline]
            fn $method(self, other: $res) -> $res {
                // forward to val-ref
                Uint{x: $imp::$method(self.x, other.x)}
            }
        }
    }
}

forward_binop!(impl Add for Uint, add);
forward_binop!(impl Sub for Uint, sub);
forward_binop!(impl Div for Uint, div);
forward_binop!(impl Rem for Uint, rem);

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
enum Value {
    bool(bool),
    Uint(Uint),
    String(String),
    u8(u8),
    u16(u16),
    u32(u32),
    u64(u64)
}
unsafe_abomonate!(Value);

fn xupd<D>(s: &Rc<RefCell<HashSet<D>>>, ds: &Rc<RefCell<HashMap<D, i8>>>, x : &D, w: isize) 
    where D:Data + Hash
{
    if w > 0 {
        let new = s.borrow_mut().insert(x.clone());
        if new {
            let f = |e: &mut i8| if *e == -1 {*e = 0;} else if *e == 0 {*e = 1};
            f(ds.borrow_mut().entry(x.clone()).or_insert(0));
        };
    } else if w < 0 {
        let present = s.borrow_mut().remove(x);
        if present {
            let f = |e: &mut i8| if *e == 1 {*e = 0;} else if *e == 0 {*e = -1;};
            f(ds.borrow_mut().entry(x.clone()).or_insert(0));
        };
    }
}

fn upd<D>(s: &Rc<RefCell<HashSet<D>>>, x: &D, w: isize) 
    where D:Data + Hash
{
    if w > 0 {
        s.borrow_mut().insert(x.clone());
    } else if w < 0 {
        s.borrow_mut().remove(x);
    }
}

fn main() {

    // start up timely computation
    timely::execute_from_args(std::env::args(), |worker| {
        let probe = probe::Handle::new();
        let mut probe1 = probe.clone();

        let mut xaction : bool = false;

        let mut _rLogicalSwitch: Rc<RefCell<HashSet<Value>>> = Rc::new(RefCell::new(HashSet::new()));
        let mut _wLogicalSwitch: Rc<RefCell<HashSet<Value>>> = _rLogicalSwitch.clone();

        let mut rels = worker.dataflow::<u64,_,_>(move |outer| {
            let (mut _LogicalSwitch, LogicalSwitch) = outer.new_collection::<Value,isize>();
            let mut __rDeltaLogicalSwitch: Rc<RefCell<HashMap<Value, i8>>> = Rc::new(RefCell::new(HashMap::new()));
            let mut __wDeltaLogicalSwitch: Rc<RefCell<HashMap<Value, i8>>> = __rDeltaLogicalSwitch.clone();
            let LogicalSwitch = LogicalSwitch.distinct();

/*            let (mut _TrunkPort, TrunkPort) = outer.new_collection::<Value,isize>();
            let TrunkPort = TrunkPort.concat(&(LogicalSwitchPort.map(|_x_| match _x_ {Value::Fact(Fact::LogicalSwitchPort(id, lswitch, ptype, name, enabled, dhcp4_options, dhcp6_options, unknown_addr, ct_zone))=> Value::Tuple(box [Value::u64(id), Value::u64(lswitch), Value::lport_type_t(ptype), Value::String(name), Value::bool(enabled), Value::opt_dhcp4_options_id_t(dhcp4_options), Value::opt_dhcp6_options_id_t(dhcp6_options), Value::bool(unknown_addr), Value::u16(ct_zone)]), _ => unreachable!()}).map(|_x_| match _x_ {Value::Tuple(box [Value::u64(ref lport), Value::u64(ref __ph0), Value::lport_type_t(ref __ph1), Value::String(ref __ph2), Value::bool(ref __ph3), Value::opt_dhcp4_options_id_t(ref __ph4), Value::opt_dhcp6_options_id_t(ref __ph5), Value::bool(ref __ph6), Value::u16(ref __ph7)]) => (Value::Tuple(box [Value::u64(lport.clone())]),Value::Tuple(box [])), _ => unreachable!()})
                                               .join_map(&(LogicalSwitchPort.map(|_x_| match _x_ {Value::Fact(Fact::LogicalSwitchPort(id, lswitch, ptype, name, enabled, dhcp4_options, dhcp6_options, unknown_addr, ct_zone))=> Value::Tuple(box [Value::u64(id), Value::u64(lswitch), Value::lport_type_t(ptype), Value::String(name), Value::bool(enabled), Value::opt_dhcp4_options_id_t(dhcp4_options), Value::opt_dhcp6_options_id_t(dhcp6_options), Value::bool(unknown_addr), Value::u16(ct_zone)]), _ => unreachable!()}).filter(|_x_| match _x_ {Value::Tuple(box [Value::u64(ref id), Value::u64(ref lswitch), Value::lport_type_t(ref ptype), Value::String(ref name), Value::bool(ref enabled), Value::opt_dhcp4_options_id_t(ref dhcp4_options), Value::opt_dhcp6_options_id_t(ref dhcp6_options), Value::bool(ref unknown_addr), Value::u16(ref ct_zone)]) => (match ptype.clone() {lport_type_t::LPortVIF{parent: _, tag_request: _, tag: _} => true, _ => false}), _ => unreachable!()}).map(|_x_| match _x_ {Value::Tuple(box [Value::u64(ref __ph8), Value::u64(ref __ph9), Value::lport_type_t(lport_type_t::LPortVIF{parent: ref lport, tag_request: ref __ph10, tag: ref __ph11}), Value::String(ref __ph12), Value::bool(ref __ph13), Value::opt_dhcp4_options_id_t(ref __ph14), Value::opt_dhcp6_options_id_t(ref __ph15), Value::bool(ref __ph16), Value::u16(ref __ph17)]) => (Value::Tuple(box [Value::u64(lport.clone())]),Value::Tuple(box [])), _ => unreachable!()})), |_x_,_y_,_z_| match (_x_,_y_,_z_) {(&Value::Tuple(box [Value::u64(ref lport)]),&Value::Tuple(box []),&Value::Tuple(box []))=> Value::Tuple(box [Value::u64(lport.clone())]), _ => unreachable!()})
                                               .map(|_x_| match _x_ {Value::Tuple(box [Value::u64(ref lport)]) => Value::Fact(Fact::TrunkPort(lport.clone())), _ => unreachable!()})));
            let TrunkPort = TrunkPort.distinct();
*/
            LogicalSwitch.inspect(move |x| xupd(&_wLogicalSwitch, &__wDeltaLogicalSwitch, &x.0, x.2)).probe_with(&mut probe1);

            [_LogicalSwitch]
        });
        
        let mut epoch = 0;
        let mut need_to_flush = false;

        fn advance(rels : &mut [InputSession<u64, Value, isize>], epoch : u64) {
            for r in rels.into_iter() {
                //print!("advance\n");
                r.advance_to(epoch);
            };
        }

        fn flush( rels : &mut [InputSession<u64, Value, isize>]
                , probe : &ProbeHandle<Product<RootTimestamp, u64>>
                , worker : &mut Root<Allocator>
                , need_to_flush : &mut bool) {
            if *need_to_flush {
                for r in rels.into_iter() {
                    //print!("flush\n");
                    r.flush();
                };
                while probe.less_than(rels[0].time()) {
                    worker.step();
                };
                *need_to_flush = false
            }
        }

       
        fn insert( rels : &mut [InputSession<u64, Value, isize>], 
                  epoch : &mut u64, 
                  rel : usize, 
                  set : &Rc<RefCell<HashSet<Value>>>, 
                  v: &Value,
                  need_to_flush: &mut bool)
        {
            if !set.borrow().contains(&v) {
                //print!("new value: {:?}\n", v);
                rels[rel].insert(v.clone());
                
                *epoch = *epoch+1;
                //print!("epoch: {}\n", epoch);
                advance(rels, *epoch);
                *need_to_flush = true
            };
        }

        fn remove (rels : &mut [InputSession<u64, Value, isize>], 
                   epoch : &mut u64, 
                   rel : usize, 
                   set : &Rc<RefCell<HashSet<Value>>>, 
                   v: &Value,
                   need_to_flush : &mut bool) 
        {
            if set.borrow().contains(&v) {
                rels[rel].remove(v.clone());
                *epoch = *epoch+1;
                advance(rels, *epoch);
                *need_to_flush = true
            };
        }

        macro_rules! delta {
            ($delta: expr) => {{
                let d = __rDeltaLogicalSwitch.borrow();
                for (f,v) in d.iter().filter(|&(_, v)| *v != 0) {
                    $delta.insert((f.field().clone(), v.clone()));
                };
            }}
        }
        macro_rules! delta_cleanup {
            () => {{
                __rDeltaLogicalSwitch.borrow_mut().clear();
            }}
        }
        macro_rules! delta_undo {
            () => {{
                let mut d = __rDeltaLogicalSwitch.borrow_mut();
                for (k,v) in d.drain() {
                    if v == 1 {
                        remove(&mut rels, &mut epoch, 0, &_rLogicalSwitch, &k, &mut need_to_flush);
                    } else if v == -1 {
                        insert(&mut rels, &mut epoch, 0, &_rLogicalSwitch, &k, &mut need_to_flush);
                    };
                };
           }}
        }

            /*
            match req {
                Request::start                       => {
                    flush(&mut rels, &probe, worker, &mut need_to_flush);
                    let resp = if xaction {
                                   Response::err(format!("transaction already in progress"))
                               } else {
                                   delta_cleanup!();
                                   xaction = true;
                                   Response::ok(())
                               };
                    serde_json::to_writer(stdout(), &resp).unwrap();
                    stdout().flush().unwrap();
                },
                Request::rollback                    => {
                    flush(&mut rels, &probe, worker, &mut need_to_flush);
                    let resp = if !xaction {
                                   Response::err(format!("no transaction in progress"))
                               } else {
                                   delta_undo!();
                                   delta_cleanup!();
                                   xaction = false;
                                   Response::ok(())
                               };
                    serde_json::to_writer(stdout(), &resp).unwrap();
                    stdout().flush().unwrap();
                },
                Request::commit                      => {
                    flush(&mut rels, &probe, worker, &mut need_to_flush);
                    let resp = if !xaction {
                                   Response::err(format!("no transaction in progress"))
                               } else {
                                   let mut delta = HashSet::new();
                                   delta!(delta);
                                   delta_cleanup!();
                                   xaction = false;
                                   Response::ok(delta)
                              };
                    serde_json::to_writer(stdout(), &resp).unwrap();
                    stdout().flush().unwrap();
                },
                Request::add(f @ Fact::LogicalSwitch(..)) => insert_resp(&mut rels, &mut epoch, 0, &_rLogicalSwitch, &Value::Fact(f), &mut need_to_flush),
                Request::del(f @ Fact::LogicalSwitch(..)) => remove_resp(&mut rels, &mut epoch, 0, &_rLogicalSwitch, &Value::Fact(f), &mut need_to_flush),
                Request::chk(Relation::LogicalSwitch) => check(&mut rels, &probe, worker, &mut need_to_flush, &_rLogicalSwitch),
                Request::enm(Relation::LogicalSwitch) => enm(&mut rels, &probe, worker, &mut need_to_flush, &_rLogicalSwitch),
            };*/
    }).unwrap();
}
