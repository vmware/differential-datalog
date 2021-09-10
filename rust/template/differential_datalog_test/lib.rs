//! Tests for the `differential_datalog` crate.
//!
//!
//! These tests live in a separate crate, as they depend on the `types` crate that defines the
//! `DDValConvert` trait.  Since `types` itself depends on `differential_datalog`, tests had to be
//! factored in a separate crate.
#![cfg_attr(not(test), allow(dead_code))]

use std::borrow::Cow;
use std::collections::btree_map::{BTreeMap, Entry};
use std::collections::btree_set::BTreeSet;
use std::sync::{Arc, Mutex};

use differential_datalog::program::config::Config;
use fnv::FnvHashMap;
use std::num::One;
use timely::communication::Allocator;
use timely::dataflow::scopes::*;
use timely::worker::Worker;

use differential_dataflow::operators::Join;
use differential_dataflow::Collection;

use differential_datalog::ddval::*;
use differential_datalog::program::*;

pub mod test_value;
use test_value::*;

const TEST_SIZE: u64 = 1000;

/*fn set_update(s: &Arc<Mutex<Delta>>, ds: &Arc<Mutex<DeltaSet<Value>>>, x : &Value, insert: bool)
{
    //println!("xupd {:?} {}", *x, w);
    if insert {
        let mut s = s.lock().unwrap();
        let new = s.insert(x.clone());
        if new {
            let mut ds = ds.lock().unwrap();
            let e = ds.entry(x.clone());
            match e {
                hash_map::Entry::Occupied(mut oe) => {
                    debug_assert!(*oe.get_mut() == -1);
                    oe.remove_entry();
                },
                hash_map::Entry::Vacant(ve) => {ve.insert(1);}
            }
        };
    } else {
        let mut s = s.lock().unwrap();
        let present = s.remove(x);
        if present {
            let mut ds = ds.lock().unwrap();
            let e = ds.entry(x.clone());
            match e {
                hash_map::Entry::Occupied(mut oe) => {
                    debug_assert!(*oe.get_mut() == 1);
                    oe.remove_entry();
                },
                hash_map::Entry::Vacant(ve) => {ve.insert(-1);}
            }
        };
    }
}*/

type Delta<T> = BTreeMap<T, Weight>;

fn set_update<T>(_rel: &str, s: &Arc<Mutex<Delta<T>>>, x: &DDValue, w: Weight)
where
    T: Ord + DDValConvert + Clone + 'static,
{
    let mut delta = s.lock().unwrap();

    let entry = delta.entry(T::from_ddvalue_ref(x).clone());
    match entry {
        Entry::Vacant(vacant) => {
            vacant.insert(w);
        }
        Entry::Occupied(mut occupied) => {
            if *occupied.get() == -w {
                occupied.remove();
            } else {
                *occupied.get_mut() += &w;
            }
        }
    };

    //println!("set_update({}) {:?} {}", rel, *x, insert);
}

/// Check that a program can be stopped multiple times without failure.
#[test]
fn test_multiple_stops() {
    let prog: Program = Program {
        nodes: Vec::new(),
        delayed_rels: Vec::new(),
        init_data: Vec::new(),
    };

    let mut running = prog.run(Config::default().with_timely_workers(2)).unwrap();
    running.stop().unwrap();
    running.stop().unwrap();
}

/// Test that we can attempt to insert a value into a non-existent
/// relation and fail gracefully.
#[test]
fn test_insert_non_existent_relation() {
    let prog: Program = Program {
        nodes: vec![],
        delayed_rels: vec![],
        init_data: vec![],
    };

    let mut running = prog.run(Config::default().with_timely_workers(3)).unwrap();
    running.transaction_start().unwrap();
    let result = running.insert(1, U64(42).into_ddvalue());
    assert_eq!(
        &result.unwrap_err(),
        "apply_update: unknown input relation 1"
    );
    running.transaction_commit().unwrap();
}

#[test]
#[should_panic(expected = "input relation (ancestor) in Scc")]
fn test_input_relation_nested() {
    let parent = {
        Relation {
            name: Cow::from("parent"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: vec![],
            change_cb: None,
        }
    };
    let ancestor = Relation {
        name: Cow::from("ancestor"),
        input: true,
        distinct: true,
        caching_mode: CachingMode::Set,
        key_func: None,
        id: 2,
        rules: vec![],
        arrangements: vec![],
        change_cb: None,
    };
    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: parent },
            ProgNode::Scc {
                rels: vec![RecursiveRelation {
                    rel: ancestor,
                    distinct: true,
                }],
            },
        ],
        delayed_rels: vec![],
        init_data: vec![],
    };

    // This call is expected to panic.
    let _err = prog
        .run(Config::default().with_timely_workers(3))
        .unwrap_err();
}

/* Test insertion/deletion into a database with a single table and no rules
 */
fn test_one_relation(nthreads: usize) {
    let relset: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel = {
        let relset1 = relset.clone();
        Relation {
            name: Cow::from("T1"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T1", &relset1, v, w))),
        }
    };

    let prog: Program = Program {
        nodes: vec![ProgNode::Rel { rel }],
        delayed_rels: vec![],
        init_data: vec![],
    };

    let mut running = prog
        .run(Config::default().with_timely_workers(nthreads))
        .unwrap();

    /* 1. Insertion */
    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set: BTreeMap<_, _> = vals.iter().map(|x| (U64(*x), Weight::One())).collect();

    running.transaction_start().unwrap();
    for x in set.keys() {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset.lock().unwrap(), set);

    /* 2. Deletion */
    let mut set2 = set.clone();
    running.transaction_start().unwrap();
    for x in set.keys() {
        set2.remove(x);
        running.delete_value(1, x.clone().into_ddvalue()).unwrap();
        //assert_eq!(running.relation_clone_content(1).unwrap(), set2);
    }
    running.transaction_commit().unwrap();
    assert_eq!(relset.lock().unwrap().len(), 0);

    /* 3. Test set semantics: insert twice, delete once */
    running.transaction_start().unwrap();
    running.insert(1, U64(1).into_ddvalue()).unwrap();
    running.insert(1, U64(1).into_ddvalue()).unwrap();
    running.delete_value(1, U64(1).into_ddvalue()).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(relset.lock().unwrap().len(), 0);

    /* 4. Set semantics: delete before insert */
    running.transaction_start().unwrap();
    running.delete_value(1, U64(1).into_ddvalue()).unwrap();
    running.insert(1, U64(1).into_ddvalue()).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(relset.lock().unwrap().len(), 1);

    /* 5. Rollback */
    let before = relset.lock().unwrap().clone();
    running.transaction_start().unwrap();
    for x in set.keys() {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
    }
    //println!("delta: {:?}", *running.relation_delta(1).unwrap().lock().unwrap());
    //    assert_eq!(*relset.lock().unwrap(), set);
    //     assert_eq!(running.relation_clone_delta(1).unwrap().len(), set.len() - 1);
    running.transaction_rollback().unwrap();

    assert_eq!(*relset.lock().unwrap(), before);

    running.stop().unwrap();
}

#[test]
fn test_one_relation_1() {
    test_one_relation(1)
}

/*
#[test]
fn test_one_relation_multi() {
    test_one_relation(16)
}*/

/* Two tables + 1 rule that keeps the two synchronized
 */
fn test_two_relations(nthreads: usize) {
    fn ifun(_v: &DDValue, _ts: TupleTS, _w: Weight) {}

    let relset1: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name: Cow::from("T1"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T1", &relset1, v, w))),
        }
    };
    let relset2: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name: Cow::from("T2"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 2,
            rules: vec![Rule::CollectionRule {
                description: Cow::Borrowed("T2.R1"),
                rel: 1,
                xform: Some(XFormCollection::Inspect {
                    description: Cow::from("Inspect"),
                    ifun: ifun as InspectFunc,
                    next: Box::new(None),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T2", &relset2, v, w))),
        }
    };

    let prog: Program = Program {
        nodes: vec![ProgNode::Rel { rel: rel1 }, ProgNode::Rel { rel: rel2 }],
        delayed_rels: vec![],
        init_data: vec![],
    };

    let mut running = prog
        .run(Config::default().with_timely_workers(nthreads))
        .unwrap();

    /* 1. Populate T1 */
    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set: BTreeMap<_, _> = vals.iter().map(|x| (U64(*x), Weight::One())).collect();

    running.transaction_start().unwrap();
    for x in set.keys() {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        //assert_eq!(running.relation_clone_content(1).unwrap(),
        //           running.relation_clone_content(2).unwrap());
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset1.lock().unwrap(), set);
    assert_eq!(*relset1.lock().unwrap(), *relset2.lock().unwrap());

    /* 2. Clear T1 */
    running.transaction_start().unwrap();
    for x in set.keys() {
        running.delete_value(1, x.clone().into_ddvalue()).unwrap();
        //        assert_eq!(running.relation_clone_content(1).unwrap(),
        //                   running.relation_clone_content(2).unwrap());
    }
    running.transaction_commit().unwrap();

    assert_eq!(relset2.lock().unwrap().len(), 0);
    assert_eq!(*relset1.lock().unwrap(), *relset2.lock().unwrap());

    /* 3. Rollback */
    running.transaction_start().unwrap();
    for x in set.keys() {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        //assert_eq!(running.relation_clone_content(1).unwrap(),
        //           running.relation_clone_content(2).unwrap());
    }
    running.transaction_rollback().unwrap();

    assert_eq!(relset1.lock().unwrap().len(), 0);
    assert_eq!(*relset1.lock().unwrap(), *relset2.lock().unwrap());

    running.stop().unwrap();
}

#[test]
fn test_two_relations_1() {
    test_two_relations(1)
}

#[test]
fn test_two_relations_multi() {
    test_two_relations(16)
}

/* Semijoin
 */
fn test_semijoin(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        Relation {
            name: Cow::from("T1"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T1", &relset1, v, w))),
        }
    };
    fn fmfun1(v: DDValue) -> Option<DDValue> {
        let Tuple2(ref v1, ref _v2) = Tuple2::<U64>::from_ddvalue(v);
        Some(v1.clone().into_ddvalue())
    }
    fn afun1(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref v1, ref v2) = Tuple2::<U64>::from_ddvalue(v);
        Some((v1.clone().into_ddvalue(), v2.clone().into_ddvalue()))
    }

    let relset2: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        Relation {
            name: Cow::from("T2"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 2,
            rules: Vec::new(),
            arrangements: vec![Arrangement::Set {
                name: Cow::from("arrange2.0"),
                fmfun: fmfun1 as FilterMapFunc,
                distinct: false,
            }],
            change_cb: Some(Arc::new(move |_, v, w| set_update("T2", &relset2, v, w))),
        }
    };
    fn jfun(_key: &DDValue, v1: &DDValue, _v2: &()) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new(U64::from_ddvalue(v1.clone())),
                Box::new(U64::from_ddvalue(v1.clone())),
            )
            .into_ddvalue(),
        )
    }

    let relset3: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name: Cow::from("T3"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 3,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T3.R1"),
                rel: 1,
                xform: Some(XFormCollection::Arrange {
                    description: Cow::from("arrange by .0"),
                    afun: afun1 as ArrangeFunc,
                    next: Box::new(XFormArrangement::Semijoin {
                        description: Cow::from("1.semijoin (2,0)"),
                        ffun: None,
                        arrangement: (2, 0),
                        jfun: jfun as SemijoinFunc,
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T3", &relset3, v, w))),
        }
    };

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: rel1 },
            ProgNode::Rel { rel: rel2 },
            ProgNode::Rel { rel: rel3 },
        ],
        delayed_rels: vec![],
        init_data: vec![],
    };

    let mut running = prog
        .run(Config::default().with_timely_workers(nthreads))
        .unwrap();

    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set: BTreeMap<_, _> = vals
        .iter()
        .map(|x| (Tuple2(Box::new(U64(*x)), Box::new(U64(*x))), Weight::One()))
        .collect();

    running.transaction_start().unwrap();
    for x in set.keys() {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        running.insert(2, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset3.lock().unwrap(), set);

    running.stop().unwrap();
}

#[test]
fn test_semijoin_1() {
    test_semijoin(1)
}

#[test]
fn test_semijoin_multi() {
    test_semijoin(16)
}

/* Inner join
 */
fn test_join(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        Relation {
            name: Cow::from("T1"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T1", &relset1, v, w))),
        }
    };
    fn afun1(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref v1, ref v2) = Tuple2::<U64>::from_ddvalue(v);
        Some((v1.clone().into_ddvalue(), v2.clone().into_ddvalue()))
    }
    let relset2: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        Relation {
            name: Cow::from("T2"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 2,
            rules: Vec::new(),
            arrangements: vec![Arrangement::Map {
                name: Cow::from("arrange2.0"),
                afun: afun1 as ArrangeFunc,
                queryable: true,
            }],
            change_cb: Some(Arc::new(move |_, v, w| set_update("T2", &relset2, v, w))),
        }
    };
    fn jfun(_key: &DDValue, v1: &DDValue, v2: &DDValue) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new((*U64::from_ddvalue_ref(v1)).clone()),
                Box::new((*U64::from_ddvalue_ref(v2)).clone()),
            )
            .into_ddvalue(),
        )
    }

    let relset3: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name: Cow::from("T3"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 3,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T3.R1"),
                rel: 1,
                xform: Some(XFormCollection::Arrange {
                    description: Cow::from("arrange by .0"),
                    afun: afun1 as ArrangeFunc,
                    next: Box::new(XFormArrangement::Join {
                        description: Cow::from("1.semijoin (2,0)"),
                        ffun: None,
                        arrangement: (2, 0),
                        jfun: jfun as JoinFunc,
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T3", &relset3, v, w))),
        }
    };

    let relset4: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel4 = {
        let relset4 = relset4.clone();
        Relation {
            name: Cow::from("T4"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 4,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T4", &relset4, v, w))),
        }
    };

    type CollectionMap<'a> =
        FnvHashMap<RelId, Collection<Child<'a, Worker<Allocator>, TS>, DDValue, Weight>>;

    fn join_transformer() -> Box<dyn for<'a> Fn(&mut CollectionMap<'a>)> {
        Box::new(|collections| {
            let rel4 = {
                let rel1 = collections.get(&1).unwrap();
                let rel1_kv = rel1.flat_map(afun1);
                let rel2 = collections.get(&2).unwrap();
                let rel2_kv = rel2.flat_map(afun1);
                rel1_kv.join(&rel2_kv).map(|(_, (v1, v2))| {
                    Tuple2(
                        Box::new(U64::from_ddvalue(v1)),
                        Box::new(U64::from_ddvalue(v2)),
                    )
                    .into_ddvalue()
                })
            };
            collections.insert(4, rel4);
        })
    }

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: rel1 },
            ProgNode::Rel { rel: rel2 },
            ProgNode::Rel { rel: rel3 },
            ProgNode::Apply {
                tfun: join_transformer,
            },
            ProgNode::Rel { rel: rel4 },
        ],
        delayed_rels: vec![],
        init_data: vec![],
    };

    let mut running = prog
        .run(Config::default().with_timely_workers(nthreads))
        .unwrap();

    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set: BTreeMap<_, _> = vals
        .iter()
        .map(|x| (Tuple2(Box::new(U64(*x)), Box::new(U64(*x))), Weight::One()))
        .collect();

    running.transaction_start().unwrap();
    for x in set.keys() {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        running.insert(2, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset3.lock().unwrap(), set);
    assert_eq!(*relset4.lock().unwrap(), set);

    let rel2dump = running.dump_arrangement((2, 0)).unwrap();
    assert_eq!(
        rel2dump,
        vals.iter().map(|x| U64(*x).into_ddvalue()).collect()
    );

    for key in vals.iter() {
        let vals = running
            .query_arrangement((2, 0), U64(*key).into_ddvalue())
            .unwrap();
        let mut expect = BTreeSet::new();
        expect.insert(U64(*key).into_ddvalue());
        assert_eq!(vals, expect);
    }

    running.stop().unwrap();
}

#[test]
fn test_join_1() {
    test_join(1)
}

#[test]
fn test_join_multi() {
    test_join(16)
}

/* Streaming join
 */
fn test_streamjoin(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        Relation {
            name: Cow::from("T1"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: vec![
                Arrangement::Map {
                    name: Cow::from("arrange1.0"),
                    afun: afun1 as ArrangeFunc,
                    queryable: false,
                },
                Arrangement::Set {
                    name: Cow::from("arrange1.1"),
                    fmfun: safun1 as FilterMapFunc,
                    distinct: false,
                },
            ],
            change_cb: Some(Arc::new(move |_, v, w| set_update("T1", &relset1, v, w))),
        }
    };

    fn afun1(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref v1, ref v2) = Tuple2::<U64>::from_ddvalue(v);
        Some((v1.clone().into_ddvalue(), v2.clone().into_ddvalue()))
    }

    fn safun1(v: DDValue) -> Option<DDValue> {
        let Tuple2(ref v1, ref _v2) = Tuple2::<U64>::from_ddvalue(v);
        Some(v1.clone().into_ddvalue())
    }

    let relset2: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        Relation {
            name: Cow::from("T2"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 2,
            rules: Vec::new(),
            arrangements: vec![],
            change_cb: Some(Arc::new(move |_, v, w| set_update("T2", &relset2, v, w))),
        }
    };
    fn jfun(v1: &DDValue, v2: &DDValue) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new(U64::from_ddvalue(v1.clone())),
                Tuple2::<U64>::from_ddvalue(v2.clone()).1,
            )
            .into_ddvalue(),
        )
    }

    fn sjfun(v1: &DDValue) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new(U64::from_ddvalue(v1.clone())),
                Box::new(U64::from_ddvalue(v1.clone())),
            )
            .into_ddvalue(),
        )
    }

    fn jfun2(v1: &DDValue, v2: &DDValue) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new(U64::from_ddvalue(v1.clone())),
                Box::new(U64::from_ddvalue(v2.clone())),
            )
            .into_ddvalue(),
        )
    }

    fn sjfun2(v1: &DDValue) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new(U64::from_ddvalue(v1.clone())),
                Box::new(U64::from_ddvalue(v1.clone())),
            )
            .into_ddvalue(),
        )
    }

    fn kfun(v: &DDValue) -> Option<DDValue> {
        let Tuple2(ref v1, ref _v2) = Tuple2::<U64>::from_ddvalue_ref(v);
        Some(v1.clone().into_ddvalue())
    }

    let relset3: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name: Cow::from("T3"),
            input: false,
            distinct: false,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 3,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T3.R1"),
                rel: 1,
                xform: Some(XFormCollection::Arrange {
                    description: Cow::from("arrange by .0"),
                    afun: afun1 as ArrangeFunc,
                    next: Box::new(XFormArrangement::StreamJoin {
                        description: Cow::from("1.streamjoin (2)"),
                        ffun: None,
                        rel: 2,
                        kfun: kfun as KeyFunc,
                        jfun: jfun as ValJoinFunc,
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T3", &relset3, v, w))),
        }
    };

    let relset4: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel4 = {
        let relset4 = relset4.clone();
        Relation {
            name: Cow::from("T4"),
            input: false,
            distinct: false,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 4,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T4.R1"),
                rel: 2,
                xform: Some(XFormCollection::StreamJoin {
                    description: Cow::from("2.streamjoin (1)"),
                    afun: afun1,
                    arrangement: (1, 0),
                    jfun: jfun2 as ValJoinFunc,
                    next: Box::new(None),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T4", &relset4, v, w))),
        }
    };

    let relset5: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel5 = {
        let relset5 = relset5.clone();
        Relation {
            name: Cow::from("T5"),
            input: false,
            distinct: false,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 5,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T5.R1"),
                rel: 1,
                xform: Some(XFormCollection::Arrange {
                    description: Cow::from("arrange by .0"),
                    afun: afun1 as ArrangeFunc,
                    next: Box::new(XFormArrangement::StreamSemijoin {
                        description: Cow::from("1.streamsemijoin (2)"),
                        ffun: None,
                        rel: 2,
                        kfun: kfun as KeyFunc,
                        jfun: sjfun as StreamSemijoinFunc,
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T5", &relset5, v, w))),
        }
    };

    let relset6: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel6 = {
        let relset6 = relset6.clone();
        Relation {
            name: Cow::from("T6"),
            input: false,
            distinct: false,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 6,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T6.R1"),
                rel: 2,
                xform: Some(XFormCollection::StreamSemijoin {
                    description: Cow::from("2.streamsemijoin (1)"),
                    afun: afun1,
                    arrangement: (1, 1),
                    jfun: sjfun2 as StreamSemijoinFunc,
                    next: Box::new(None),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T6", &relset6, v, w))),
        }
    };

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: rel1 },
            ProgNode::Rel { rel: rel2 },
            ProgNode::Rel { rel: rel3 },
            ProgNode::Rel { rel: rel4 },
            ProgNode::Rel { rel: rel5 },
            ProgNode::Rel { rel: rel6 },
        ],
        delayed_rels: vec![],
        init_data: vec![],
    };

    let mut running = prog
        .run(Config::default().with_timely_workers(nthreads))
        .unwrap();

    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set: BTreeMap<_, _> = vals
        .iter()
        .map(|x| (Tuple2(Box::new(U64(*x)), Box::new(U64(*x))), 1))
        .collect();

    running.transaction_start().unwrap();
    for x in set.keys() {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        running.insert(2, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset3.lock().unwrap(), set);
    assert_eq!(*relset4.lock().unwrap(), set);
    assert_eq!(*relset5.lock().unwrap(), set);
    assert_eq!(*relset6.lock().unwrap(), set);

    running.stop().unwrap();
}

#[test]
fn test_streamjoin_1() {
    test_streamjoin(1)
}

#[test]
fn test_streamjoin_multi() {
    test_streamjoin(16)
}
/* Antijoin
 */
fn test_antijoin(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        Relation {
            name: Cow::from("T1"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T1", &relset1, v, w))),
        }
    };
    fn afun1(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref v1, _) = Tuple2::<U64>::from_ddvalue_ref(&v);
        Some(((*v1).clone().into_ddvalue(), v.clone()))
    }

    let relset2: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        Relation {
            name: Cow::from("T2"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 2,
            rules: Vec::new(),
            arrangements: vec![],
            change_cb: Some(Arc::new(move |_, v, w| set_update("T2", &relset2, v, w))),
        }
    };

    fn mfun(v: DDValue) -> DDValue {
        let Tuple2(ref v1, _) = Tuple2::<U64>::from_ddvalue(v);
        v1.clone().into_ddvalue()
    }

    fn fmnull_fun(v: DDValue) -> Option<DDValue> {
        Some(v)
    }

    let relset21: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel21 = {
        Relation {
            name: Cow::from("T21"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 21,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T21.R1"),
                rel: 2,
                xform: Some(XFormCollection::Map {
                    description: Cow::from("map by .0"),
                    mfun: mfun as MapFunc,
                    next: Box::new(None),
                }),
            }],
            arrangements: vec![Arrangement::Set {
                name: Cow::from("arrange2.1"),
                fmfun: fmnull_fun as FilterMapFunc,
                distinct: true,
            }],
            change_cb: Some(Arc::new(move |_, v, w| set_update("T21", &relset21, v, w))),
        }
    };

    let relset3: Arc<Mutex<Delta<Tuple2<U64>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name: Cow::from("T3"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 3,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T3.R1"),
                rel: 1,
                xform: Some(XFormCollection::Arrange {
                    description: Cow::from("arrange by .0"),
                    afun: afun1 as ArrangeFunc,
                    next: Box::new(XFormArrangement::Antijoin {
                        description: Cow::from("1.antijoin (21,0)"),
                        ffun: None,
                        arrangement: (21, 0),
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T3", &relset3, v, w))),
        }
    };

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: rel1 },
            ProgNode::Rel { rel: rel2 },
            ProgNode::Rel { rel: rel21 },
            ProgNode::Rel { rel: rel3 },
        ],
        delayed_rels: vec![],
        init_data: vec![],
    };

    let mut running = prog
        .run(Config::default().with_timely_workers(nthreads))
        .unwrap();

    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set: BTreeMap<_, _> = vals
        .iter()
        .map(|x| (Tuple2(Box::new(U64(*x)), Box::new(U64(*x))), 1))
        .collect();

    /* 1. T2 and T1 contain identical keys; antijoin is empty */
    running.transaction_start().unwrap();
    for x in set.keys() {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
        running.insert(2, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(relset3.lock().unwrap().len(), 0);

    /* 1. T2 is empty; antijoin is identical to T1 */
    running.transaction_start().unwrap();
    for x in set.keys() {
        running.delete_value(2, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();
    assert_eq!(*relset3.lock().unwrap(), set);

    running.stop().unwrap();
}

#[test]
fn test_antijoin_1() {
    test_antijoin(1)
}

#[test]
fn test_antijoin_multi() {
    test_antijoin(16)
}

/* Maps and filters
 */
fn test_map(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name: Cow::from("T1"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T1", &relset1, v, w))),
        }
    };

    fn mfun(v: DDValue) -> DDValue {
        let &U64(uv) = U64::from_ddvalue_ref(&v);
        U64(uv * 2).into_ddvalue()
    }

    fn gfun3(v: DDValue) -> Option<(DDValue, DDValue)> {
        Some((Empty {}.into_ddvalue(), v))
    }

    fn agfun3(_key: &DDValue, src: &[(&DDValue, Weight)]) -> Option<DDValue> {
        Some(U64(src.len() as u64).into_ddvalue())
    }

    fn gfun4(v: DDValue) -> Option<(DDValue, DDValue)> {
        Some((v.clone(), v))
    }

    fn agfun4(key: &DDValue, _src: &[(&DDValue, Weight)]) -> Option<DDValue> {
        Some(key.clone())
    }

    fn ffun(v: &DDValue) -> bool {
        let U64(ref uv) = U64::from_ddvalue_ref(v);
        *uv > 10
    }

    fn fmfun(v: DDValue) -> Option<DDValue> {
        let &U64(uv) = U64::from_ddvalue_ref(&v);
        if uv > 12 {
            Some(U64(uv * 2).into_ddvalue())
        } else {
            None
        }
    }

    fn flatmapfun(v: DDValue) -> Option<Box<dyn Iterator<Item = DDValue>>> {
        let &U64(i) = U64::from_ddvalue_ref(&v);
        if i > 12 {
            Some(Box::new(
                vec![
                    I64(-(i as i64)).into_ddvalue(),
                    I64(-(2 * (i as i64))).into_ddvalue(),
                ]
                .into_iter(),
            ))
        } else {
            None
        }
    }

    let relset2: Arc<Mutex<Delta<I64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name: Cow::from("T2"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 2,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T2.R1"),
                rel: 1,
                xform: Some(XFormCollection::Map {
                    description: Cow::from("map x2"),
                    mfun: mfun as MapFunc,
                    next: Box::new(Some(XFormCollection::Filter {
                        description: Cow::from("filter >10"),
                        ffun: ffun as FilterFunc,
                        next: Box::new(Some(XFormCollection::FilterMap {
                            description: Cow::from("filter >12 map x2"),
                            fmfun: fmfun as FilterMapFunc,
                            next: Box::new(Some(XFormCollection::FlatMap {
                                description: Cow::from("flat-map >12 [-x,-2x]"),
                                fmfun: flatmapfun as FlatMapFunc,
                                next: Box::new(None),
                            })),
                        })),
                    })),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T2", &relset2, v, w))),
        }
    };
    let relset3: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name: Cow::from("T3"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 3,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T3.R1"),
                rel: 2,
                xform: Some(XFormCollection::Arrange {
                    description: Cow::from("arrange by ()"),
                    afun: gfun3 as ArrangeFunc,
                    next: Box::new(XFormArrangement::Aggregate {
                        description: Cow::from("2.aggregate"),
                        ffun: None,
                        aggfun: agfun3 as AggFunc,
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T3", &relset3, v, w))),
        }
    };

    let relset4: Arc<Mutex<Delta<I64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel4 = {
        let relset4 = relset4.clone();
        Relation {
            name: Cow::from("T4"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 4,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T4.R1"),
                rel: 2,
                xform: Some(XFormCollection::Arrange {
                    description: Cow::from("arrange by self"),
                    afun: gfun4 as ArrangeFunc,
                    next: Box::new(XFormArrangement::Aggregate {
                        description: Cow::from("2.aggregate"),
                        ffun: None,
                        aggfun: agfun4 as AggFunc,
                        next: Box::new(None),
                    }),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T4", &relset4, v, w))),
        }
    };

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: rel1 },
            ProgNode::Rel { rel: rel2 },
            ProgNode::Rel { rel: rel3 },
            ProgNode::Rel { rel: rel4 },
        ],
        delayed_rels: vec![],
        init_data: vec![],
    };

    let mut running = prog
        .run(Config::default().with_timely_workers(nthreads))
        .unwrap();

    let vals: Vec<u64> = (0..TEST_SIZE).collect();
    let set: BTreeMap<_, _> = vals.iter().map(|x| (U64(*x), 1)).collect();
    let expected2: BTreeMap<_, _> = vals
        .iter()
        .map(|x| U64(*x).into_ddvalue())
        .map(mfun)
        .filter(ffun)
        .filter_map(fmfun)
        .flat_map(|x| match flatmapfun(x) {
            Some(iter) => iter,
            None => Box::new(None.into_iter()),
        })
        .map(|x| (I64::from_ddvalue(x), 1))
        .collect();
    let mut expected3 = BTreeMap::default();
    expected3.insert(U64(expected2.len() as u64), 1);

    running.transaction_start().unwrap();
    for x in set.keys() {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();

    assert_eq!(*relset1.lock().unwrap(), set);
    assert_eq!(*relset2.lock().unwrap(), expected2);
    assert_eq!(*relset3.lock().unwrap(), expected3);
    assert_eq!(*relset4.lock().unwrap(), expected2);
    running.stop().unwrap();
}

#[test]
fn test_map_1() {
    test_map(1)
}

#[test]
fn test_map_multi() {
    test_map(16)
}

/* Delayed relations.
 */
fn test_delayed(nthreads: usize) {
    let relset1: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel1 = {
        Relation {
            name: Cow::from("T1"),
            input: true,
            distinct: false,
            caching_mode: CachingMode::Stream,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T1", &relset1, v, w))),
        }
    };

    let relset2: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name: Cow::from("T2"),
            input: false,
            distinct: false,
            caching_mode: CachingMode::Stream,
            key_func: None,
            id: 2,
            rules: vec![
                Rule::CollectionRule {
                    description: Cow::from("T2 :- T1|-1"),
                    rel: 11,
                    xform: None,
                },
                Rule::CollectionRule {
                    description: Cow::from("T2 :- T3"),
                    rel: 3,
                    xform: None,
                },
            ],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T2", &relset2, v, w))),
        }
    };

    // Empty relation to union with T1|-1 to prevent outputs from showing up ahead of time.
    let relset3: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel3 = {
        Relation {
            name: Cow::from("T3"),
            input: true,
            distinct: false,
            caching_mode: CachingMode::Stream,
            key_func: None,
            id: 3,
            rules: Vec::new(),
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T3", &relset3, v, w))),
        }
    };

    let relset4: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel4 = {
        let relset4 = relset4.clone();
        Relation {
            name: Cow::from("T4"),
            input: false,
            distinct: false,
            caching_mode: CachingMode::Stream,
            key_func: None,
            id: 4,
            rules: vec![
                Rule::CollectionRule {
                    description: Cow::from("T4 :- T1"),
                    rel: 1,
                    xform: None,
                },
                Rule::CollectionRule {
                    description: Cow::from("T4 :- T1|-1"),
                    rel: 11,
                    xform: None,
                },
            ],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T4", &relset4, v, w))),
        }
    };

    let relset5: Arc<Mutex<Delta<U64>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let rel5 = {
        let relset5 = relset5.clone();
        Relation {
            name: Cow::from("T5"),
            input: false,
            distinct: false,
            caching_mode: CachingMode::Stream,
            key_func: None,
            id: 5,
            rules: vec![Rule::CollectionRule {
                description: Cow::from("T5 :- T1.stream_xform(arrange)."),
                rel: 1,
                xform: Some(XFormCollection::StreamXForm {
                    description: Cow::from("T1.stream_xform"),
                    xform: Box::new(Some(XFormCollection::Arrange {
                        description: Cow::from("T1.stream_arrange"),
                        afun: afun1,
                        next: Box::new(XFormArrangement::FilterMap {
                            description: Cow::from("T5 :-"),
                            fmfun: fmfun1,
                            next: Box::new(None),
                        }),
                    })),
                    next: Box::new(None),
                }),
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| set_update("T5", &relset5, v, w))),
        }
    };
    fn afun1(v: DDValue) -> Option<(DDValue, DDValue)> {
        let v = U64::from_ddvalue(v);
        Some((v.clone().into_ddvalue(), v.into_ddvalue()))
    }

    fn fmfun1(v: DDValue) -> Option<DDValue> {
        Some(v)
    }

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: rel1 },
            ProgNode::Rel { rel: rel3 },
            ProgNode::Rel { rel: rel2 },
            ProgNode::Rel { rel: rel4 },
            ProgNode::Rel { rel: rel5 },
        ],
        delayed_rels: vec![DelayedRelation {
            id: 11,
            rel_id: 1,
            delay: 1,
        }],
        init_data: vec![],
    };

    let mut running = prog
        .run(Config::default().with_timely_workers(nthreads))
        .unwrap();

    for i in 0..TEST_SIZE {
        println!("Round {}", i);
        running.transaction_start().unwrap();
        running.insert(1, U64(i).into_ddvalue()).unwrap();
        running.transaction_commit().unwrap();
        // T2 contains all values in T1 delayed by 1 epoch
        let expected: BTreeMap<_, _> = (0..i).map(|x| (U64(x), 1)).collect();
        assert_eq!(*relset2.lock().unwrap(), expected);
        // T4 contains all values in T1 twice, except the last value, which occurs once.
        let expected: BTreeMap<_, _> = (0..i + 1)
            .map(|x| (U64(x), if x == i { 1 } else { 2 }))
            .collect();
        assert_eq!(*relset4.lock().unwrap(), expected);
        // T5 contains all values.
        let expected: BTreeMap<_, _> = (0..i + 1).map(|x| (U64(x), 1)).collect();
        assert_eq!(*relset5.lock().unwrap(), expected);
    }

    running.stop().unwrap();
}

#[test]
fn test_delayed_1() {
    test_delayed(1)
}

#[test]
fn test_delayed_multi() {
    test_delayed(16)
}

/* Recursion
*/
fn test_recursion(nthreads: usize) {
    fn arrange_by_fst(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref fst, ref snd) = Tuple2::<String>::from_ddvalue_ref(&v);
        Some(((*fst).clone().into_ddvalue(), (*snd).clone().into_ddvalue()))
    }

    fn arrange_by_snd(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref fst, ref snd) = Tuple2::<String>::from_ddvalue_ref(&v);
        Some((
            (**snd).clone().into_ddvalue(),
            (**fst).clone().into_ddvalue(),
        ))
    }

    fn anti_arrange1(v: DDValue) -> Option<(DDValue, DDValue)> {
        Some((v.clone(), v))
    }

    fn anti_arrange2(v: DDValue) -> Option<(DDValue, DDValue)> {
        let Tuple2(ref fst, ref snd) = Tuple2::<String>::from_ddvalue_ref(&v);
        Some((
            Tuple2(Box::new((**snd).clone()), Box::new((**fst).clone())).into_ddvalue(),
            v.clone(),
        ))
    }

    fn jfun(_parent: &DDValue, ancestor: &DDValue, child: &DDValue) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new(String::from_ddvalue_ref(ancestor).clone()),
                Box::new(String::from_ddvalue_ref(child).clone()),
            )
            .into_ddvalue(),
        )
    }

    fn jfun2(_ancestor: &DDValue, descendant1: &DDValue, descendant2: &DDValue) -> Option<DDValue> {
        Some(
            Tuple2(
                Box::new(String::from_ddvalue_ref(descendant1).clone()),
                Box::new(String::from_ddvalue_ref(descendant2).clone()),
            )
            .into_ddvalue(),
        )
    }

    fn fmnull_fun(v: DDValue) -> Option<DDValue> {
        Some(v)
    }

    let parentset: Arc<Mutex<Delta<Tuple2<String>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let parent = {
        let parentset = parentset.clone();
        Relation {
            name: Cow::from("parent"),
            input: true,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 1,
            rules: Vec::new(),
            arrangements: vec![Arrangement::Map {
                name: Cow::from("arrange_by_parent"),
                afun: arrange_by_fst as ArrangeFunc,
                queryable: false,
            }],
            change_cb: Some(Arc::new(move |_, v, w| {
                set_update("parent", &parentset, v, w)
            })),
        }
    };

    let ancestorset: Arc<Mutex<Delta<Tuple2<String>>>> = Arc::new(Mutex::new(BTreeMap::default()));
    let ancestor = {
        let ancestorset = ancestorset.clone();
        Relation {
            name: Cow::from("ancestor"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 2,
            rules: vec![
                Rule::CollectionRule {
                    description: Cow::from("ancestor.R1"),
                    rel: 1,
                    xform: None,
                },
                Rule::ArrangementRule {
                    description: Cow::from("ancestor.R2"),
                    arr: (2, 2),
                    xform: XFormArrangement::Join {
                        description: Cow::from("(2,2).join (1,0)"),
                        ffun: None,
                        arrangement: (1, 0),
                        jfun: jfun as JoinFunc,
                        next: Box::new(None),
                    },
                },
            ],
            arrangements: vec![
                Arrangement::Map {
                    name: Cow::from("arrange_by_ancestor"),
                    afun: arrange_by_fst as ArrangeFunc,
                    queryable: false,
                },
                Arrangement::Set {
                    name: Cow::from("arrange_by_self"),
                    fmfun: fmnull_fun as FilterMapFunc,
                    distinct: true,
                },
                Arrangement::Map {
                    name: Cow::from("arrange_by_snd"),
                    afun: arrange_by_snd as ArrangeFunc,
                    queryable: false,
                },
            ],
            change_cb: Some(Arc::new(move |_, v, w| {
                set_update("ancestor", &ancestorset, v, w)
            })),
        }
    };

    fn ffun(v: &DDValue) -> bool {
        let Tuple2(ref fst, ref snd) = Tuple2::<String>::from_ddvalue_ref(v);
        *fst != *snd
    }

    let common_ancestorset: Arc<Mutex<Delta<Tuple2<String>>>> =
        Arc::new(Mutex::new(BTreeMap::default()));
    let common_ancestor = {
        let common_ancestorset = common_ancestorset.clone();
        Relation {
            name: Cow::from("common_ancestor"),
            input: false,
            distinct: true,
            caching_mode: CachingMode::Set,
            key_func: None,
            id: 3,
            rules: vec![Rule::ArrangementRule {
                description: Cow::from("common_ancestor.R1"),
                arr: (2, 0),
                xform: XFormArrangement::Join {
                    description: Cow::from("(2,0).join (2,0)"),
                    ffun: None,
                    arrangement: (2, 0),
                    jfun: jfun2 as JoinFunc,
                    next: Box::new(Some(XFormCollection::Arrange {
                        description: Cow::from("arrange by self"),
                        afun: anti_arrange1 as ArrangeFunc,
                        next: Box::new(XFormArrangement::Antijoin {
                            description: Cow::from("(2,0).antijoin (2,1)"),
                            ffun: None,
                            arrangement: (2, 1),
                            next: Box::new(Some(XFormCollection::Arrange {
                                description: Cow::from("arrange by .1"),
                                afun: anti_arrange2 as ArrangeFunc,
                                next: Box::new(XFormArrangement::Antijoin {
                                    description: Cow::from("...join (2,1)"),
                                    ffun: None,
                                    arrangement: (2, 1),
                                    next: Box::new(Some(XFormCollection::Filter {
                                        description: Cow::from("filter .0 != .1"),
                                        ffun: ffun as FilterFunc,
                                        next: Box::new(None),
                                    })),
                                }),
                            })),
                        }),
                    })),
                },
            }],
            arrangements: Vec::new(),
            change_cb: Some(Arc::new(move |_, v, w| {
                set_update("common_ancestor", &common_ancestorset, v, w)
            })),
        }
    };

    let prog: Program = Program {
        nodes: vec![
            ProgNode::Rel { rel: parent },
            ProgNode::Scc {
                rels: vec![RecursiveRelation {
                    rel: ancestor,
                    distinct: true,
                }],
            },
            ProgNode::Rel {
                rel: common_ancestor,
            },
        ],
        delayed_rels: vec![],
        init_data: vec![],
    };

    let mut running = prog
        .run(Config::default().with_timely_workers(nthreads))
        .unwrap();

    /* 1. Populate parent relation */
    /*
        A-->B-->C
        |   |-->D
        |
        |-->E
    */
    let vals = vec![
        Tuple2::new(String("A".to_string()), String("B".to_string())),
        Tuple2::new(String("B".to_string()), String("C".to_string())),
        Tuple2::new(String("B".to_string()), String("D".to_string())),
        Tuple2::new(String("A".to_string()), String("E".to_string())),
    ];
    let set: BTreeMap<_, _> = vals.iter().map(|x| (x.clone(), 1)).collect();

    let expect_vals = vec![
        Tuple2::new(String("A".to_string()), String("B".to_string())),
        Tuple2::new(String("B".to_string()), String("C".to_string())),
        Tuple2::new(String("B".to_string()), String("D".to_string())),
        Tuple2::new(String("A".to_string()), String("E".to_string())),
        Tuple2::new(String("A".to_string()), String("D".to_string())),
        Tuple2::new(String("A".to_string()), String("C".to_string())),
    ];

    let expect_set: BTreeMap<_, _> = expect_vals.iter().map(|x| (x.clone(), 1)).collect();

    let expect_vals2 = vec![
        Tuple2::new(String("C".to_string()), String("D".to_string())),
        Tuple2::new(String("D".to_string()), String("C".to_string())),
        Tuple2::new(String("C".to_string()), String("E".to_string())),
        Tuple2::new(String("E".to_string()), String("C".to_string())),
        Tuple2::new(String("D".to_string()), String("E".to_string())),
        Tuple2::new(String("E".to_string()), String("D".to_string())),
        Tuple2::new(String("E".to_string()), String("B".to_string())),
        Tuple2::new(String("B".to_string()), String("E".to_string())),
    ];
    let expect_set2: BTreeMap<_, _> = expect_vals2.iter().map(|x| (x.clone(), 1)).collect();

    running.transaction_start().unwrap();
    for x in set.keys() {
        running.insert(1, x.clone().into_ddvalue()).unwrap();
    }
    running.transaction_commit().unwrap();
    //println!("commit done");

    assert_eq!(*parentset.lock().unwrap(), set);
    assert_eq!(*ancestorset.lock().unwrap(), expect_set);
    assert_eq!(*common_ancestorset.lock().unwrap(), expect_set2);

    /* 2. Remove record from "parent" relation */
    running.transaction_start().unwrap();
    running
        .delete_value(1, vals[0].clone().into_ddvalue())
        .unwrap();
    running.transaction_commit().unwrap();

    let expect_vals3 = vec![
        Tuple2::new(String("B".to_string()), String("C".to_string())),
        Tuple2::new(String("B".to_string()), String("D".to_string())),
        Tuple2::new(String("A".to_string()), String("E".to_string())),
    ];
    let expect_set3: BTreeMap<_, _> = expect_vals3.iter().map(|x| (x.clone(), 1)).collect();

    assert_eq!(*ancestorset.lock().unwrap(), expect_set3);

    let expect_vals4 = vec![
        Tuple2::new(String("C".to_string()), String("D".to_string())),
        Tuple2::new(String("D".to_string()), String("C".to_string())),
    ];
    let expect_set4: BTreeMap<_, _> = expect_vals4.iter().map(|x| (x.clone(), 1)).collect();

    assert_eq!(*common_ancestorset.lock().unwrap(), expect_set4);

    /* 3. Test clear_relation() */
    running.transaction_start().unwrap();
    running.clear_relation(1).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(*parentset.lock().unwrap(), BTreeMap::default());
    assert_eq!(*ancestorset.lock().unwrap(), BTreeMap::default());
    assert_eq!(*common_ancestorset.lock().unwrap(), BTreeMap::default());

    running.stop().unwrap();
}

#[test]
fn test_recursion_1() {
    test_recursion(1)
}

#[test]
fn test_recursion_multi() {
    test_recursion(16)
}

#[test]
fn conversion_lossless() {
    let boolean = Bool(true);
    let val = boolean.clone().into_ddvalue();

    assert_eq!(Some(&boolean), Bool::try_from_ddvalue_ref(&val));
    assert_eq!(Some(boolean.clone()), Bool::try_from_ddvalue(val.clone()));
    assert_eq!(&boolean, Bool::from_ddvalue_ref(&val));
    assert_eq!(boolean, Bool::from_ddvalue(val));
}

#[test]
fn checked_conversions() {
    let val = Bool(true).into_ddvalue();

    assert!(Empty::try_from_ddvalue_ref(&val).is_none());
    assert!(Empty::try_from_ddvalue(val).is_none());
}

#[test]
#[should_panic(expected = "attempted to convert a DDValue into the incorrect type")]
fn incorrect_from_type() {
    let val = Bool(true).into_ddvalue();

    let _panic = Empty::from_ddvalue(val);
}

#[test]
#[should_panic(expected = "attempted to convert a DDValue into the incorrect type")]
fn incorrect_from_ref_type() {
    let val = Bool(true).into_ddvalue();

    let _panic = Empty::from_ddvalue_ref(&val);
}
