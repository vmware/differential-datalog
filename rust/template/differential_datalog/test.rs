#![allow(non_snake_case, non_camel_case_types, non_upper_case_globals, dead_code)]

use program::*;
use uint::*;
use abomonation::Abomonation;

use std::sync::{Arc,Mutex};
use fnv::{FnvHashSet, FnvHashMap};
use std::iter::FromIterator;
use differential_dataflow::Collection;
use differential_dataflow::operators::Join;
use timely::dataflow::scopes::*;
use timely::communication::Allocator;
use timely::worker::Worker;

const TEST_SIZE: u64 = 1000;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
struct P {
    f1: Q,
    f2: bool
}
unsafe_abomonate!(P);

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
struct Q {
    f1: bool,
    f2: String
}
unsafe_abomonate!(Q);


#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
enum S {
    S1 {f1: u32, f2: String, f3: Q, f4: Uint},
    S2 {e1: bool},
    S3 {g1: Q, g2: Q}
}
unsafe_abomonate!(S);

impl S {
    fn f1(&mut self) -> &mut u32 {
        match self {
            S::S1{ref mut f1,..} => f1,
            _         => panic!("")
        }
    }
}

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
enum Value {
    empty(),
    bool(bool),
    Uint(Uint),
    String(String),
    u8(u8),
    u16(u16),
    u32(u32),
    u64(u64),
    i64(i64),
    BoolTuple((bool, bool)),
    Tuple2(Box<Value>, Box<Value>),
    Q(Q),
    S(S)
}
unsafe_abomonate!(Value);

impl Default for Value {
    fn default() -> Value {Value::bool(false)}
}

fn _filter_fun1(v: &Value) -> bool {
    match &v {
        Value::S(S::S1{f1: _, f2: _, f3: _, f4: _}) => true, 
        _ => false
    }
}

fn some_fun(x: &u32) -> u32 {
    (*x)+1
}

fn _arrange_fun1(v: Value) -> Option<(Value, Value)> {
    let (x, _2) = match &v {
        Value::S(S::S1{f1: ref x, f2: _0, f3: _2, f4: _3}) if *_0 == "foo".to_string() && *_3 == (Uint::from_u64(32) & Uint::from_u64(0xff)) => (x, _2), 
        _ => return None
    };
    if (*_2).f1 && (*x) + 1 > 5 { return None; };
    if some_fun(x) > 5 { return None; };
    if some_fun(&((*x)+1)) > 5 { return None; };
    if {let y=5; some_fun(&y) > 5} { return None; };
    if {let x = 0; x < 4} { return None; };
    if (*_2 == Q{f1: _2.f1, f2: _2.f2.clone()}) { return None; };
    if {let v = &((*x) + 1); (*v) > 0} { return None; };
    if {let v = &(*_2).f1; *v} { return None; };
    if {let V = _2.f1; V} { return None; };
    if {
        let s = &mut S::S1{f1: 0, f2: "foo".to_string(), f3: Q{f1: true, f2: "bar".to_string()}, f4: Uint::from_u64(10)};
        *s.f1() = 5;
        let q = s.f1();
        (*q) > 0
        //q == Q{f1: false, f2: "buzz".to_string()}
    } {
        return None;
    };
    if {
        let ref mut p = P{f1: Q{f1: true, f2: "x".to_string()}, f2: true};
        let ref mut b = false;
        p.f1 = Q{f1: true, f2: "x".to_string()};
        let ref mut _pf1 = p.f1.clone();
        let ref mut _pf11 = p.f1.clone();
        let ref mut _pf2 = p.f1.f1 || p.f1.f1;
        let ref mut _pf22 = p.f2.clone();
        let ref mut z = true;
        let ref mut _pclone = p.clone();
        let Q{f1: ref mut qf1, f2: ref mut qf2} = p.f1.clone();
        *qf1 = true;
        let ref mut neq = Q{f1: qf1.clone(), f2: qf2.clone()};
        *z = true;

        //*f2 = false.clone();
        //*f1 = Q{f1: true, f2: "x".to_string()};
        let (ref mut _v1, ref mut _v2): (Q,bool) = (Q{f1: true, f2: "x".to_string()}, false);
        (*b) = false;

        let ref mut s = S::S1{f1: 0, f2: "f2".to_string(), f3: neq.clone(), f4: Uint::from_u64(10)};
        match s {
            S::S1{f1,f2: _,f3: _,f4: _} => {
                *f1 = 2; ()
            },
            _         => return None
        };
        /*
        match p {
            P{f1, f2: true} => *f1 = p.f1.clone(),
            _               => return None
        };*/
        /*match &mut p.f1 {
            Q{f1: true, f2} => *f2 = p.f1.f2.clone(),
            _               => return None
        };*/
        let ref mut a: u64 = 5 as u64;
        let ref mut b: u64 = 5;
        let ref mut _c: u64 = *(a)+*(b);
        *a = *a+*a+*a;
        let ref mut str1: String = ("str1".to_string()) as String;
        let ref mut str2 = "str2".to_string();
        let ref mut _str3 = (*str1).push_str(str2.as_str());
        let (ref mut _str4, _) = ("str4".to_string(), "str5".to_string());
        match &(a,b) {
            (5, _) => (),
            _  => return None
        }
        *z
    } { return None ;};
    match *_2 {
        Q{f1: true, f2: _} => {},
        _ => return None
    }
    Some((Value::S(S::S3{g1: _2.clone(), g2: _2.clone()}), v.clone()))
}

/*
fn arrange_fun1(v: Value) -> Option<(Value, Value)> {
    match v {
        Value::Tuple2(v1,v2) => Some((*v1, *v2)),
        _ => None
    }
}*/

/*fn set_update(s: &Arc<Mutex<ValSet<Value>>>, ds: &Arc<Mutex<DeltaSet<Value>>>, x : &Value, insert: bool)
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

fn set_update(_rel: &str, s: &Arc<Mutex<ValSet<Value>>>, x : &Value, w: bool)
{
    //println!("set_update({}) {:?} {}", rel, *x, insert);
    if w {
        s.lock().unwrap().insert(x.clone());
    } else {
        s.lock().unwrap().remove(x);
    }
}

/* Test insertion/deletion into a database with a single table and no rules
 */
fn test_one_relation(nthreads: usize) {
    let relset: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel = {
        let relset1 = relset.clone();
        Relation {
            name:         "T1".to_string(),
            input:        true,
            distinct:     true,
            key_func:     None,
            id:           1,
            rules:        Vec::new(),
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T1", &relset1, v, pol)))))
        }
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::Rel{rel}],
        init_data: vec![]
    };

    let mut running = prog.run(nthreads);

    /* 1. Insertion */
    let vals:Vec<u64> = (0..TEST_SIZE).collect();
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::u64(*x)));

    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
    };
    running.transaction_commit().unwrap();

    assert_eq!(*relset.lock().unwrap(), set);

    /* 2. Deletion */
    let mut set2 = set.clone();
    running.transaction_start().unwrap();
    for x in &set {
        set2.remove(x);
        running.delete_value(1, x.clone()).unwrap();
        //assert_eq!(running.relation_clone_content(1).unwrap(), set2);
    };
    running.transaction_commit().unwrap();
    assert_eq!(relset.lock().unwrap().len(), 0);

    /* 3. Test set semantics: insert twice, delete once */
    running.transaction_start().unwrap();
    running.insert(1, Value::u64(1)).unwrap();
    running.insert(1, Value::u64(1)).unwrap();
    running.delete_value(1, Value::u64(1)).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(relset.lock().unwrap().len(), 0);

    /* 4. Set semantics: delete before insert */
    running.transaction_start().unwrap();
    running.delete_value(1, Value::u64(1)).unwrap();
    running.insert(1, Value::u64(1)).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(relset.lock().unwrap().len(), 1);

    /* 5. Rollback */
    let before = relset.lock().unwrap().clone();
    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
    };
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
    let relset1: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name:         "T1".to_string(),
            input:        true,
            distinct:     true,
            key_func:     None,
            id:           1,
            rules:        Vec::new(),
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T1", &relset1, v, pol)))))
        }
    };
    let relset2: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name:         "T2".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           2,
            rules:        vec![Rule::CollectionRule{description: "T2.R1".to_string(), rel: 1, xform: None}],
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T2", &relset2, v, pol)))))
        }
    };

    let prog: Program<Value> = Program {
        nodes: vec![
            ProgNode::Rel{rel: rel1},
            ProgNode::Rel{rel: rel2}],
        init_data: vec![]
    };

    let mut running = prog.run(nthreads);

    /* 1. Populate T1 */
    let vals:Vec<u64> = (0..TEST_SIZE).collect();
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::u64(*x)));

    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
        //assert_eq!(running.relation_clone_content(1).unwrap(),
        //           running.relation_clone_content(2).unwrap());
    };
    running.transaction_commit().unwrap();

    assert_eq!(*relset1.lock().unwrap(), set);
    assert_eq!(*relset1.lock().unwrap(), *relset2.lock().unwrap());

    /* 2. Clear T1 */
    running.transaction_start().unwrap();
    for x in &set {
        running.delete_value(1, x.clone()).unwrap();
//        assert_eq!(running.relation_clone_content(1).unwrap(),
//                   running.relation_clone_content(2).unwrap());
    };
    running.transaction_commit().unwrap();

    assert_eq!(relset2.lock().unwrap().len(), 0);
    assert_eq!(*relset1.lock().unwrap(), *relset2.lock().unwrap());

    /* 3. Rollback */
    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
        //assert_eq!(running.relation_clone_content(1).unwrap(),
        //           running.relation_clone_content(2).unwrap());
    };
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
    let relset1: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name:         "T1".to_string(),
            input:        true,
            distinct:     true,
            key_func:     None,
            id:           1,
            rules:        Vec::new(),
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T1", &relset1, v, pol)))))
        }
    };
    fn fmfun1(v: Value) -> Option<Value> {
        match v {
            Value::Tuple2(v1,_v2) => Some(*v1),
            _ => None
        }
    }
    fn afun1(v: Value) -> Option<(Value, Value)> {
        match v {
            Value::Tuple2(v1,v2) => Some((*v1, *v2)),
            _ => None
        }
    }

    let relset2: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name:         "T2".to_string(),
            input:        true,
            distinct:     true,
            key_func:     None,
            id:           2,
            rules:        Vec::new(),
            arrangements: vec![Arrangement::Set{
                name: "arrange2.0".to_string(),
                fmfun: &(fmfun1 as FilterMapFunc<Value>),
                distinct: false
            }],
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T2", &relset2, v, pol)))))
        }
    };
    fn jfun(_key: &Value, v1: &Value, _v2: &()) -> Option<Value> {
        Some(Value::Tuple2(Box::new(v1.clone()), Box::new(v1.clone())))
    }

    let relset3: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name:         "T3".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           3,
            rules:        vec![
                Rule::CollectionRule{
                    description: "T3.R1".to_string(),
                    rel: 1,
                    xform: Some(
                        XFormCollection::Arrange{
                            description: "arrange by .0".to_string(),
                            afun: &(afun1 as ArrangeFunc<Value>),
                            next: Box::new(XFormArrangement::Semijoin{
                                description: "1.semijoin (2,0)".to_string(),
                                ffun:        None,
                                arrangement: (2,0),
                                jfun:        &(jfun as SemijoinFunc<Value>),
                                next:        Box::new(None)
                            })
                        })
                }],
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T3", &relset3, v, pol)))))
        }
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::Rel{rel: rel1},
                    ProgNode::Rel{rel: rel2},
                    ProgNode::Rel{rel: rel3}],
        init_data: vec![]
    };

    let mut running = prog.run(nthreads);

    let vals:Vec<u64> = (0..TEST_SIZE).collect();
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::Tuple2(Box::new(Value::u64(*x)),Box::new(Value::u64(*x)))));

    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
        running.insert(2, x.clone()).unwrap();
    };
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
    let relset1: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name:         "T1".to_string(),
            input:        true,
            distinct:     true,
            key_func:     None,
            id:           1,
            rules:        Vec::new(),
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T1", &relset1, v, pol)))))
        }
    };
    fn afun1(v: Value) -> Option<(Value, Value)> {
        match v {
            Value::Tuple2(v1,v2) => Some((*v1, *v2)),
            _ => None
        }
    }
    let relset2: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name:         "T2".to_string(),
            input:        true,
            distinct:     true,
            key_func:     None,
            id:           2,
            rules:        Vec::new(),
            arrangements: vec![Arrangement::Map{
                name: "arrange2.0".to_string(),
                afun: &(afun1 as ArrangeFunc<Value>)
            }],
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T2", &relset2, v, pol)))))
        }
    };
    fn jfun(_key: &Value, v1: &Value, v2: &Value) -> Option<Value> {
        Some(Value::Tuple2(Box::new(v1.clone()), Box::new(v2.clone())))
    }

    let relset3: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name:         "T3".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           3,
            rules:        vec![
                Rule::CollectionRule{
                    description: "T3.R1".to_string(),
                    rel: 1,
                    xform: Some(
                        XFormCollection::Arrange {
                            description: "arrange by .0".to_string(),
                            afun: &(afun1 as ArrangeFunc<Value>),
                            next: Box::new(XFormArrangement::Join{
                                description: "1.semijoin (2,0)".to_string(),
                                ffun:        None,
                                arrangement: (2,0),
                                jfun:        &(jfun as JoinFunc<Value>),
                                next:        Box::new(None)
                            })
                        })
                }],
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T3", &relset3, v, pol)))))
        }
    };

    let relset4: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel4 = {
        let relset4 = relset4.clone();
        Relation {
            name:         "T4".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           4,
            rules:        Vec::new(),
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T4", &relset4, v, pol)))))
        }
    };

    fn join_transformer() -> Box<for<'a> Fn(&mut FnvHashMap<RelId, Collection<Child<'a, Worker<Allocator>, TS>,Value,Weight>>)> {
        Box::new(|collections| {
            let rel4 = {
                let rel1 = collections.get(&1).unwrap();
                let rel1_kv = rel1.flat_map(afun1);
                let rel2 = collections.get(&2).unwrap();
                let rel2_kv = rel2.flat_map(afun1);
                rel1_kv.join(&rel2_kv).map(|(_,(v1,v2))| Value::Tuple2(Box::new(v1), Box::new(v2)))
            };
            collections.insert(4, rel4);
        })
    }

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::Rel{rel: rel1},
                    ProgNode::Rel{rel: rel2},
                    ProgNode::Rel{rel: rel3},
                    ProgNode::Apply{tfun: join_transformer},
                    ProgNode::Rel{rel: rel4}
                    ],
        init_data: vec![]
    };


    let mut running = prog.run(nthreads);

    let vals:Vec<u64> = (0..TEST_SIZE).collect();
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::Tuple2(Box::new(Value::u64(*x)),Box::new(Value::u64(*x)))));

    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
        running.insert(2, x.clone()).unwrap();
    };
    running.transaction_commit().unwrap();

    assert_eq!(*relset3.lock().unwrap(), set);
    assert_eq!(*relset4.lock().unwrap(), set);

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

/* Antijoin
 */
fn test_antijoin(nthreads: usize) {
    let relset1: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name:         "T1".to_string(),
            input:        true,
            distinct:     true,
            key_func:     None,
            id:           1,
            rules:        Vec::new(),
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T1", &relset1, v, pol)))))
        }
    };
    fn afun1(v: Value) -> Option<(Value, Value)> {
        let (v1,v) = match &v {
            Value::Tuple2(v1,_) => ((**v1).clone(), v.clone()),
            _ => return None
        };
        Some((v1,v))
    }

    fn _afunx(v: Value) -> Option<(Value, Value)> {
        let (v1,v2): (&bool, &bool) = match &v {
            Value::BoolTuple((v1,v2)) => (v1,v2),
            _ => return None
        };
        Some((Value::bool(*v1),Value::bool(*v2)))
    }

    let relset2: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name:         "T2".to_string(),
            input:        true,
            distinct:     true,
            key_func:     None,
            id:           2,
            rules:        Vec::new(),
            arrangements: vec![],
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T2", &relset2, v, pol)))))
        }
    };

    fn mfun(v: Value) -> Value {
        match v {
            Value::Tuple2(v1,_) => *v1,
            _ => panic!("unexpected value")
        }
    }

    fn fmnull_fun(v: Value) -> Option<Value> {
        Some(v)
    }

    let relset21: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel21 = {
        let relset21 = relset21.clone();
        Relation {
            name:         "T21".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           21,
            rules:        vec![
                Rule::CollectionRule{
                    description: "T21.R1".to_string(),
                    rel: 2,
                    xform: Some(
                        XFormCollection::Map{
                            description: "map by .0".to_string(),
                            mfun: &(mfun as MapFunc<Value>),
                            next: Box::new(None)
                        })
                }],
            arrangements: vec![
                Arrangement::Set{
                    name: "arrange2.1".to_string(),
                    fmfun: &(fmnull_fun as FilterMapFunc<Value>),
                    distinct: true
                }],
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T21", &relset21, v, pol)))))
        }
    };

    let relset3: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name:         "T3".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           3,
            rules:        vec![
                Rule::CollectionRule {
                    description: "T3.R1".to_string(),
                    rel: 1,
                    xform: Some(
                        XFormCollection::Arrange{
                            description: "arrange by .0".to_string(),
                            afun: &(afun1 as ArrangeFunc<Value>),
                            next: Box::new(
                                XFormArrangement::Antijoin{
                                    description: "1.antijoin (21,0)".to_string(),
                                    ffun: None,
                                    arrangement: (21,0),
                                    next: Box::new(None)
                                })
                        })
                }],
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T3", &relset3, v, pol)))))
        }
    };

    let prog: Program<Value> = Program {
        nodes: vec![
            ProgNode::Rel{rel: rel1},
            ProgNode::Rel{rel: rel2},
            ProgNode::Rel{rel: rel21},
            ProgNode::Rel{rel: rel3}],
        init_data: vec![]
    };

    let mut running = prog.run(nthreads);

    let vals:Vec<u64> = (0..TEST_SIZE).collect();
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::Tuple2(Box::new(Value::u64(*x)),Box::new(Value::u64(*x)))));

    /* 1. T2 and T1 contain identical keys; antijoin is empty */
    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
        running.insert(2, x.clone()).unwrap();
    };
    running.transaction_commit().unwrap();

    assert_eq!(relset3.lock().unwrap().len(), 0);

    /* 1. T2 is empty; antijoin is identical to T1 */
    running.transaction_start().unwrap();
    for x in &set {
        running.delete_value(2, x.clone()).unwrap();
    };
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
    let relset1: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel1 = {
        let relset1 = relset1.clone();
        Relation {
            name:         "T1".to_string(),
            input:        true,
            distinct:     true,
            key_func:     None,
            id:           1,
            rules:        Vec::new(),
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T1", &relset1, v, pol)))))
        }
    };

    fn mfun(v: Value) -> Value {
        match &v {
            Value::u64(uv) => Value::u64(uv * 2),
            _ => v.clone()
        }
    }

    fn gfun3(v: Value) -> Option<(Value, Value)> {
        Some((Value::empty(), v))
    }

    fn agfun3(_key: &Value, src: &[(&Value, Weight)]) -> Value {
        Value::u64(src.len() as u64)
    }

    fn gfun4(v: Value) -> Option<(Value, Value)> { Some((v.clone(), v)) }

    fn agfun4(key: &Value, _src: &[(&Value, Weight)]) -> Value {
        key.clone()
    }

    fn ffun(v: &Value) -> bool {
        match &v {
            Value::u64(uv) => *uv > 10,
            _ => false
        }
    }

    fn fmfun(v: Value) -> Option<Value> {
        match &v {
            Value::u64(uv) => {
                if *uv > 12 {
                    Some(Value::u64((*uv) * 2))
                } else { None }
            },
            _ => None
        }
    }

    fn flatmapfun(v: Value) -> Option<Box<Iterator<Item=Value>>> {
        match &v {
            Value::u64(i) => {
                if *i > 12 {
                    Some(Box::new(vec![Value::i64(-(*i as i64)), Value::i64(-(2*(*i as i64)))].into_iter()))
                } else { None }
            },
            _ => None
        }
    }

    let relset2: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel2 = {
        let relset2 = relset2.clone();
        Relation {
            name:         "T2".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           2,
            rules:        vec![Rule::CollectionRule{
                description: "T2.R1".to_string(),
                rel: 1,
                xform: Some(
                    XFormCollection::Map{
                        description: "map x2".to_string(),
                        mfun: &(mfun as MapFunc<Value>),
                        next: Box::new(Some(
                                XFormCollection::Filter{
                                    description: "filter >10".to_string(),
                                    ffun: &(ffun as FilterFunc<Value>),
                                    next: Box::new(Some(
                                            XFormCollection::FilterMap{
                                                description: "filter >12 map x2".to_string(),
                                                fmfun: &(fmfun as FilterMapFunc<Value>),
                                                next: Box::new(Some(
                                                        XFormCollection::FlatMap{
                                                            description: "flat-map >12 [-x,-2x]".to_string(),
                                                            fmfun: &(flatmapfun as FlatMapFunc<Value>),
                                                            next: Box::new(None)
                                                        }))
                                            }))
                                }))
                    })
            }],
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T2", &relset2, v, pol)))))
        }
    };
    let relset3: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel3 = {
        let relset3 = relset3.clone();
        Relation {
            name:         "T3".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           3,
            rules:        vec![Rule::CollectionRule{
                description: "T3.R1".to_string(),
                rel: 2,
                xform: Some(
                    XFormCollection::Arrange {
                        description: "arrange by ()".to_string(),
                        afun: &(gfun3 as ArrangeFunc<Value>),
                        next: Box::new(XFormArrangement::Aggregate{
                            description: "2.aggregate".to_string(),
                            ffun: None,
                            aggfun: &(agfun3 as AggFunc<Value>),
                            next: Box::new(None)
                        })
                    })
            }],
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T3", &relset3, v, pol)))))
        }
    };

    let relset4: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let rel4 = {
        let relset4 = relset4.clone();
        Relation {
            name:         "T4".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           4,
            rules:        vec![Rule::CollectionRule{
                description: "T4.R1".to_string(),
                rel: 2,
                xform: Some(
                    XFormCollection::Arrange {
                        description: "arrange by self".to_string(),
                        afun: &(gfun4 as ArrangeFunc<Value>),
                        next: Box::new(XFormArrangement::Aggregate {
                            description: "2.aggregate".to_string(),
                            ffun: None,
                            aggfun: &(agfun4 as AggFunc<Value>),
                            next: Box::new(None)
                        })
                    })
            }],
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("T4", &relset4, v, pol)))))
        }
    };


    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::Rel{rel: rel1},
                    ProgNode::Rel{rel: rel2},
                    ProgNode::Rel{rel: rel3},
                    ProgNode::Rel{rel: rel4}],
        init_data: vec![]
    };

    let mut running = prog.run(nthreads);

    let vals:Vec<u64> = (0..TEST_SIZE).collect();
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::u64(*x)));
    let expected2 = FnvHashSet::from_iter(vals.iter().
                                          map(|x| Value::u64(*x)).
                                          map(|x| mfun(x)).
                                          filter(|x| ffun(&x)).
                                          filter_map(|x| fmfun(x)).
                                          flat_map(|x| match flatmapfun(x) {Some(iter) => iter, None => Box::new(None.into_iter())} ));
    let mut expected3 = FnvHashSet::default();
    expected3.insert(Value::u64(expected2.len() as u64));

    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
    };
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


/* Recursion
*/
fn test_recursion(nthreads: usize) {
    fn arrange_by_fst(v: Value) -> Option<(Value, Value)> {
        match &v {
            Value::Tuple2(fst,snd) => Some(((**fst).clone(), (**snd).clone())),
            _ => None
        }
    }
    fn arrange_by_snd(v: Value) -> Option<(Value, Value)> {
        match &v {
            Value::Tuple2(fst,snd) => Some(((**snd).clone(), (**fst).clone())),
            _ => None
        }
    }
    fn anti_arrange1(v: Value) -> Option<(Value, Value)> {
        //println!("anti_arrange({:?})", v);
        let res = Some((v.clone(),v.clone()));
        //println!("res: {:?}", res);
        res
    }

    fn anti_arrange2(v: Value) -> Option<(Value, Value)> {
        match &v {
            Value::Tuple2(fst,snd) => Some((Value::Tuple2(Box::new((**snd).clone()), Box::new((**fst).clone())), v.clone())),
            _ => None
        }
    }

    fn jfun(_parent: &Value, ancestor: &Value, child: &Value) -> Option<Value> {
        Some(Value::Tuple2(Box::new(ancestor.clone()), Box::new(child.clone())))
    }

    fn jfun2(_ancestor: &Value, descendant1: &Value, descendant2: &Value) -> Option<Value> {
        Some(Value::Tuple2(Box::new(descendant1.clone()), Box::new(descendant2.clone())))
    }

    fn fmnull_fun(v: Value) -> Option<Value> {
        Some(v)
    }

    let parentset: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let parent = {
        let parentset = parentset.clone();
        Relation {
            name:         "parent".to_string(),
            input:        true,
            distinct:     true,
            key_func:     None,
            id:           1,
            rules:        Vec::new(),
            arrangements: vec![Arrangement::Map{
                name: "arrange_by_parent".to_string(),
                afun: &(arrange_by_fst as ArrangeFunc<Value>)
            }],
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("parent", &parentset, v, pol)))))
        }
    };

    let ancestorset: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let ancestor = {
        let ancestorset = ancestorset.clone();
        Relation {
            name:         "ancestor".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           2,
            rules:        vec![
                Rule::CollectionRule{
                    description: "ancestor.R1".to_string(),
                    rel: 1,
                    xform: None
                },
                Rule::ArrangementRule{
                    description: "ancestor.R2".to_string(),
                    arr: (2,2),
                    xform: XFormArrangement::Join{
                        description: "(2,2).join (1,0)".to_string(),
                        ffun:        None,
                        arrangement: (1,0),
                        jfun: &(jfun as JoinFunc<Value>),
                        next: Box::new(None)
                    }
                }],
            arrangements: vec![
                Arrangement::Map{
                    name: "arrange_by_ancestor".to_string(),
                    afun: &(arrange_by_fst as ArrangeFunc<Value>)
                },
                Arrangement::Set{
                    name: "arrange_by_self".to_string(),
                    fmfun: &(fmnull_fun as FilterMapFunc<Value>),
                    distinct: true
                },
                Arrangement::Map{
                    name: "arrange_by_snd".to_string(),
                    afun: &(arrange_by_snd as ArrangeFunc<Value>)
                }
            ],
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("ancestor", &ancestorset, v, pol)))))
        }
    };

    fn ffun(v: &Value) -> bool {
        match &v {
            Value::Tuple2(fst,snd) => fst != snd,
            _ => false
        }
    }

    let common_ancestorset: Arc<Mutex<ValSet<Value>>> = Arc::new(Mutex::new(FnvHashSet::default()));
    let common_ancestor = {
        let common_ancestorset = common_ancestorset.clone();
        Relation {
            name:         "common_ancestor".to_string(),
            input:        false,
            distinct:     true,
            key_func:     None,
            id:           3,
            rules:        vec![
                Rule::ArrangementRule{
                    description: "common_ancestor.R1".to_string(),
                    arr: (2,0),
                    xform: XFormArrangement::Join{
                        description: "(2,0).join (2,0)".to_string(),
                        ffun:        None,
                        arrangement: (2,0),
                        jfun: &(jfun2 as JoinFunc<Value>),
                        next: Box::new(Some(XFormCollection::Arrange{
                            description: "arrange by self".to_string(),
                            afun:        &(anti_arrange1 as ArrangeFunc<Value>),
                            next: Box::new(XFormArrangement::Antijoin {
                                description: "(2,0).antijoin (2,1)".to_string(),
                                ffun: None,
                                arrangement: (2,1),
                                next: Box::new(Some(XFormCollection::Arrange{
                                    description: "arrange by .1".to_string(),
                                    afun:        &(anti_arrange2 as ArrangeFunc<Value>),
                                    next: Box::new(XFormArrangement::Antijoin{
                                        description: "...join (2,1)".to_string(),
                                        ffun: None,
                                        arrangement: (2,1),
                                        next: Box::new(Some(XFormCollection::Filter{
                                            description: "filter .0 != .1".to_string(),
                                            ffun: &(ffun as FilterFunc<Value>),
                                            next: Box::new(None)
                                        }))
                                    })
                                }))
                            })
                        }))
                    }
                }],
            arrangements: Vec::new(),
            change_cb:    Some(Arc::new(Mutex::new(Box::new(move |_,v,pol| set_update("common_ancestor", &common_ancestorset, v, pol)))))
        }
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::Rel{rel: parent},
                    ProgNode::SCC{rels: vec![ancestor]},
                    ProgNode::Rel{rel: common_ancestor}],
        init_data: vec![]
    };

    let mut running = prog.run(nthreads);

    /* 1. Populate parent relation */
    /*
        A-->B-->C
        |   |-->D
        |
        |-->E
    */
    let vals = vec![Value::Tuple2(Box::new(Value::String("A".to_string())), Box::new(Value::String("B".to_string()))),
                    Value::Tuple2(Box::new(Value::String("B".to_string())), Box::new(Value::String("C".to_string()))),
                    Value::Tuple2(Box::new(Value::String("B".to_string())), Box::new(Value::String("D".to_string()))),
                    Value::Tuple2(Box::new(Value::String("A".to_string())), Box::new(Value::String("E".to_string())))];
    let set = FnvHashSet::from_iter(vals.iter().map(|x| x.clone()));

    let expect_vals = vec![Value::Tuple2(Box::new(Value::String("A".to_string())), Box::new(Value::String("B".to_string()))),
                           Value::Tuple2(Box::new(Value::String("B".to_string())), Box::new(Value::String("C".to_string()))),
                           Value::Tuple2(Box::new(Value::String("B".to_string())), Box::new(Value::String("D".to_string()))),
                           Value::Tuple2(Box::new(Value::String("A".to_string())), Box::new(Value::String("E".to_string()))),
                           Value::Tuple2(Box::new(Value::String("A".to_string())), Box::new(Value::String("D".to_string()))),
                           Value::Tuple2(Box::new(Value::String("A".to_string())), Box::new(Value::String("C".to_string())))];

    let expect_set = FnvHashSet::from_iter(expect_vals.iter().map(|x| x.clone()));

    let expect_vals2 = vec![Value::Tuple2(Box::new(Value::String("C".to_string())), Box::new(Value::String("D".to_string()))),
                            Value::Tuple2(Box::new(Value::String("D".to_string())), Box::new(Value::String("C".to_string()))),
                            Value::Tuple2(Box::new(Value::String("C".to_string())), Box::new(Value::String("E".to_string()))),
                            Value::Tuple2(Box::new(Value::String("E".to_string())), Box::new(Value::String("C".to_string()))),
                            Value::Tuple2(Box::new(Value::String("D".to_string())), Box::new(Value::String("E".to_string()))),
                            Value::Tuple2(Box::new(Value::String("E".to_string())), Box::new(Value::String("D".to_string()))),
                            Value::Tuple2(Box::new(Value::String("E".to_string())), Box::new(Value::String("B".to_string()))),
                            Value::Tuple2(Box::new(Value::String("B".to_string())), Box::new(Value::String("E".to_string())))];
    let expect_set2 = FnvHashSet::from_iter(expect_vals2.iter().map(|x| x.clone()));

    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
    };
    running.transaction_commit().unwrap();
    //println!("commit done");

    assert_eq!(*parentset.lock().unwrap(), set);
    assert_eq!(*ancestorset.lock().unwrap(), expect_set);
    assert_eq!(*common_ancestorset.lock().unwrap(), expect_set2);

    /* 2. Remove record from "parent" relation */
    running.transaction_start().unwrap();
    running.delete_value(1, vals[0].clone()).unwrap();
    running.transaction_commit().unwrap();

    let expect_vals3 = vec![Value::Tuple2(Box::new(Value::String("B".to_string())), Box::new(Value::String("C".to_string()))),
                            Value::Tuple2(Box::new(Value::String("B".to_string())), Box::new(Value::String("D".to_string()))),
                            Value::Tuple2(Box::new(Value::String("A".to_string())), Box::new(Value::String("E".to_string())))];
    let expect_set3 = FnvHashSet::from_iter(expect_vals3.iter().map(|x| x.clone()));

    assert_eq!(*ancestorset.lock().unwrap(), expect_set3);

    let expect_vals4 = vec![Value::Tuple2(Box::new(Value::String("C".to_string())), Box::new(Value::String("D".to_string()))),
                            Value::Tuple2(Box::new(Value::String("D".to_string())), Box::new(Value::String("C".to_string())))];
    let expect_set4 = FnvHashSet::from_iter(expect_vals4.iter().map(|x| x.clone()));

    assert_eq!(*common_ancestorset.lock().unwrap(), expect_set4);

    /* 3. Test clear_relation() */
    running.transaction_start().unwrap();
    running.clear_relation(1).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(*parentset.lock().unwrap(), FnvHashSet::default());
    assert_eq!(*ancestorset.lock().unwrap(), FnvHashSet::default());
    assert_eq!(*common_ancestorset.lock().unwrap(), FnvHashSet::default());


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
