#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_shorthand_field_patterns)]
#![allow(unused_variables)]
use abomonation::Abomonation;
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
use std::fmt::Debug;
use std;

use timely;
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

use program::*;

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

fn runProgram<V:Val>(prog: &Program<V>) {

    // start up timely computation
    timely::execute_from_args(std::env::args(), |worker| {
        let probe = probe::Handle::new();
        let mut probe1 = probe.clone();

        let mut xaction : bool = false;

        let mut _rLogicalSwitch: Rc<RefCell<HashSet<V>>> = Rc::new(RefCell::new(HashSet::new()));
        let mut _wLogicalSwitch: Rc<RefCell<HashSet<V>>> = _rLogicalSwitch.clone();

        let mut rels = worker.dataflow::<u64,_,_>(move |outer| {
            let (mut _LogicalSwitch, LogicalSwitch) = outer.new_collection::<V,isize>();
            let mut __rDeltaLogicalSwitch: Rc<RefCell<HashMap<V, i8>>> = Rc::new(RefCell::new(HashMap::new()));
            let mut __wDeltaLogicalSwitch: Rc<RefCell<HashMap<V, i8>>> = __rDeltaLogicalSwitch.clone();
            let LogicalSwitch = LogicalSwitch.distinct();

/*            let (mut _TrunkPort, TrunkPort) = outer.new_collection::<V,isize>();
            let TrunkPort = TrunkPort.concat(&(LogicalSwitchPort.map(|_x_| match _x_ {V::Fact(Fact::LogicalSwitchPort(id, lswitch, ptype, name, enabled, dhcp4_options, dhcp6_options, unknown_addr, ct_zone))=> V::Tuple(box [V::u64(id), V::u64(lswitch), V::lport_type_t(ptype), V::String(name), V::bool(enabled), V::opt_dhcp4_options_id_t(dhcp4_options), V::opt_dhcp6_options_id_t(dhcp6_options), V::bool(unknown_addr), V::u16(ct_zone)]), _ => unreachable!()}).map(|_x_| match _x_ {V::Tuple(box [V::u64(ref lport), V::u64(ref __ph0), V::lport_type_t(ref __ph1), V::String(ref __ph2), V::bool(ref __ph3), V::opt_dhcp4_options_id_t(ref __ph4), V::opt_dhcp6_options_id_t(ref __ph5), V::bool(ref __ph6), V::u16(ref __ph7)]) => (V::Tuple(box [V::u64(lport.clone())]),V::Tuple(box [])), _ => unreachable!()})
                                               .join_map(&(LogicalSwitchPort.map(|_x_| match _x_ {V::Fact(Fact::LogicalSwitchPort(id, lswitch, ptype, name, enabled, dhcp4_options, dhcp6_options, unknown_addr, ct_zone))=> V::Tuple(box [V::u64(id), V::u64(lswitch), V::lport_type_t(ptype), V::String(name), V::bool(enabled), V::opt_dhcp4_options_id_t(dhcp4_options), V::opt_dhcp6_options_id_t(dhcp6_options), V::bool(unknown_addr), V::u16(ct_zone)]), _ => unreachable!()}).filter(|_x_| match _x_ {V::Tuple(box [V::u64(ref id), V::u64(ref lswitch), V::lport_type_t(ref ptype), V::String(ref name), V::bool(ref enabled), V::opt_dhcp4_options_id_t(ref dhcp4_options), V::opt_dhcp6_options_id_t(ref dhcp6_options), V::bool(ref unknown_addr), V::u16(ref ct_zone)]) => (match ptype.clone() {lport_type_t::LPortVIF{parent: _, tag_request: _, tag: _} => true, _ => false}), _ => unreachable!()}).map(|_x_| match _x_ {V::Tuple(box [V::u64(ref __ph8), V::u64(ref __ph9), V::lport_type_t(lport_type_t::LPortVIF{parent: ref lport, tag_request: ref __ph10, tag: ref __ph11}), V::String(ref __ph12), V::bool(ref __ph13), V::opt_dhcp4_options_id_t(ref __ph14), V::opt_dhcp6_options_id_t(ref __ph15), V::bool(ref __ph16), V::u16(ref __ph17)]) => (V::Tuple(box [V::u64(lport.clone())]),V::Tuple(box [])), _ => unreachable!()})), |_x_,_y_,_z_| match (_x_,_y_,_z_) {(&V::Tuple(box [V::u64(ref lport)]),&V::Tuple(box []),&V::Tuple(box []))=> V::Tuple(box [V::u64(lport.clone())]), _ => unreachable!()})
                                               .map(|_x_| match _x_ {V::Tuple(box [V::u64(ref lport)]) => V::Fact(Fact::TrunkPort(lport.clone())), _ => unreachable!()})));
            let TrunkPort = TrunkPort.distinct();
*/
            LogicalSwitch.inspect(move |x| xupd(&_wLogicalSwitch, &__wDeltaLogicalSwitch, &x.0, x.2)).probe_with(&mut probe1);

            [_LogicalSwitch]
        });
        
        let mut epoch = 0;
        let mut need_to_flush = false;

        /*
        fn advance<V: Val>(rels : &mut [InputSession<u64, V, isize>], epoch : u64) {
            for r in rels.into_iter() {
                //print!("advance\n");
                r.advance_to(epoch);
            };
        }

        fn flush<V: Val>( rels : &mut [InputSession<u64, V, isize>]
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

       
        fn insert<V: Val>( rels : &mut [InputSession<u64, V, isize>], 
                  epoch : &mut u64, 
                  rel : usize, 
                  set : &Rc<RefCell<HashSet<V>>>, 
                  v: &V,
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

        fn remove<V: Val>(rels : &mut [InputSession<u64, V, isize>], 
                   epoch : &mut u64, 
                   rel : usize, 
                   set : &Rc<RefCell<HashSet<V>>>, 
                   v: &V,
                   need_to_flush : &mut bool) 
        {
            if set.borrow().contains(&v) {
                rels[rel].remove(v.clone());
                *epoch = *epoch+1;
                advance(rels, *epoch);
                *need_to_flush = true
            };
        }*/

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
                Request::add(f @ Fact::LogicalSwitch(..)) => insert_resp(&mut rels, &mut epoch, 0, &_rLogicalSwitch, &V::Fact(f), &mut need_to_flush),
                Request::del(f @ Fact::LogicalSwitch(..)) => remove_resp(&mut rels, &mut epoch, 0, &_rLogicalSwitch, &V::Fact(f), &mut need_to_flush),
                Request::chk(Relation::LogicalSwitch) => check(&mut rels, &probe, worker, &mut need_to_flush, &_rLogicalSwitch),
                Request::enm(Relation::LogicalSwitch) => enm(&mut rels, &probe, worker, &mut need_to_flush, &_rLogicalSwitch),
            };*/
    }).unwrap();
}

/*
                              - join function: arrange input 1;
                                               call join_core with a closure that
                                                   - filters the second input relation,
                                                   - compute output tuple
                                                   - filters output (by returning option)
                  all arrangements used in rules
*/



