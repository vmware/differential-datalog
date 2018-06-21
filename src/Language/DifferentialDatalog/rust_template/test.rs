#![allow(non_camel_case_types)]

#[cfg(test)]

use program::*;
use uint::*;
use abomonation::Abomonation;

#[cfg(test)]
use fnv::FnvHashSet;
#[cfg(test)]
use std::iter::FromIterator;

#[cfg(test)]
const TEST_SIZE: u64 = 1000;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
enum Value {
    bool(bool),
    Uint(Uint),
    String(String),
    u8(u8),
    u16(u16),
    u32(u32),
    u64(u64),
    Tuple2(Box<Value>, Box<Value>)
}
unsafe_abomonate!(Value);

impl Default for Value {
    fn default() -> Value {Value::bool(false)}
}

/* Test insertion/deletion into a database with a single table and no rules
 */
#[cfg(test)]
fn test_one_relation(nthreads: usize) {
    let rel = Relation {
        name:         "T1".to_string(),
        input:        true,
        id:           1,      
        rules:        Vec::new(),
        arrangements: Vec::new()
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::RelNode{rel}]
    };

    let mut running = prog.run(nthreads);

    /* 1. Insertion */
    let vals:Vec<u64> = (0..TEST_SIZE).collect();
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::u64(*x)));

    for x in &set {
        running.transaction_start().unwrap();
        running.insert(1, x.clone()).unwrap();
        running.transaction_commit().unwrap();
    };

    assert_eq!(running.relation_clone_content(1).unwrap(), set);

    /* 2. Deletion */
    let mut set2 = set.clone();
    running.transaction_start().unwrap();
    for x in &set {
        set2.remove(x);
        running.delete(1, x.clone()).unwrap();
        assert_eq!(running.relation_clone_content(1).unwrap(), set2);
    };
    running.transaction_commit().unwrap();
    assert_eq!(running.relation_clone_content(1).unwrap().len(), 0);

    /* 3. Test set semantics: insert twice, delete once */
    running.transaction_start().unwrap();
    running.insert(1, Value::u64(1)).unwrap();
    running.insert(1, Value::u64(1)).unwrap();
    running.delete(1, Value::u64(1)).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(running.relation_clone_content(1).unwrap().len(), 0);

    /* 4. Set semantics: delete before insert */
    running.transaction_start().unwrap();
    running.delete(1, Value::u64(1)).unwrap();
    running.insert(1, Value::u64(1)).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(running.relation_clone_content(1).unwrap().len(), 1);

    /* 5. Rollback */
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::u64(*x)));

    let before = running.relation_clone_content(1).unwrap().clone();
    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
    };
    //println!("delta: {:?}", *running.relation_delta(1).unwrap().lock().unwrap());
    assert_eq!(running.relation_clone_content(1).unwrap(), set);
    assert_eq!(running.relation_clone_delta(1).unwrap().len(), set.len() - 1);
    running.transaction_rollback().unwrap();

    assert_eq!(running.relation_clone_content(1).unwrap(), before);

    running.stop().unwrap();
}


#[test]
fn test_one_relation_1() {
    test_one_relation(1)
}

/*
#[test]
fn test_one_relation_16() {
    test_one_relation(16)
}*/

/* Two tables + 1 rule that keeps the two synchronized
 */
#[cfg(test)]
fn test_two_relations(nthreads: usize) {
    let rel1 = Relation {
        name:         "T1".to_string(),
        input:        true,
        id:           1,      
        rules:        Vec::new(),
        arrangements: Vec::new()
    };
    let rel2 = Relation {
        name:         "T2".to_string(),
        input:        false,
        id:           2,      
        rules:        vec![Rule{rel: 1, xforms: Vec::new()}],
        arrangements: Vec::new()
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::RelNode{rel: rel1},
                    ProgNode::RelNode{rel: rel2}]
    };

    let mut running = prog.run(nthreads);

    /* 1. Populate T1 */
    let vals:Vec<u64> = (0..TEST_SIZE).collect();
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::u64(*x)));

    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
        assert_eq!(running.relation_clone_content(1).unwrap(), 
                   running.relation_clone_content(2).unwrap());
    };
    running.transaction_commit().unwrap();

    assert_eq!(running.relation_clone_content(1).unwrap(), set);
    assert_eq!(running.relation_clone_content(1).unwrap(), 
               running.relation_clone_content(2).unwrap());

    /* 2. Clear T1 */
    running.transaction_start().unwrap();
    for x in &set {
        running.delete(1, x.clone()).unwrap();
        assert_eq!(running.relation_clone_content(1).unwrap(), 
                   running.relation_clone_content(2).unwrap());
    };
    running.transaction_commit().unwrap();

    assert_eq!(running.relation_clone_content(2).unwrap().len(), 0);
    assert_eq!(running.relation_clone_content(1).unwrap(), 
               running.relation_clone_content(2).unwrap());

    /* 3. Rollback */
    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
        assert_eq!(running.relation_clone_content(1).unwrap(), 
                   running.relation_clone_content(2).unwrap());
    };
    running.transaction_rollback().unwrap();

    assert_eq!(running.relation_clone_content(1).unwrap().len(), 0);
    assert_eq!(running.relation_clone_content(1).unwrap(), 
               running.relation_clone_content(2).unwrap());

    running.stop().unwrap();
}

#[test]
fn test_two_relations_1() {
    test_two_relations(1)
}

/*
#[test]
fn test_two_relations_16() {
    test_two_relations(16)
}*/


/* Inner join
 */
#[cfg(test)]
fn test_join(nthreads: usize) {
    let rel1 = Relation {
        name:         "T1".to_string(),
        input:        true,
        id:           1,      
        rules:        Vec::new(),
        arrangements: Vec::new()
    };
    fn afun1(v: Value) -> Option<(Value, Value)> {
        match v {
            Value::Tuple2(v1,v2) => Some((*v1, *v2)),
            _ => None
        }
    }
    let rel2 = Relation {
        name:         "T2".to_string(),
        input:        true,
        id:           2,      
        rules:        Vec::new(),
        arrangements: vec![Arrangement{
            name: "arrange2.0".to_string(),
            afun: &(afun1 as ArrangeFunc<Value>)
        }]
    };
    fn jfun(_key: &Value, v1: &Value, v2: &Value) -> Option<Value> {
        Some(Value::Tuple2(Box::new(v1.clone()), Box::new(v2.clone())))
    }

    let rel3 = Relation {
        name:         "T3".to_string(),
        input:        false,
        id:           3,
        rules:        vec![Rule{
            rel: 1, 
            xforms: vec![XForm::Join{
               afun:        &(afun1 as ArrangeFunc<Value>),
               arrangement: (2,0),
               jfun:        &(jfun as JoinFunc<Value>)
            }]
        }],
        arrangements: Vec::new()
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::RelNode{rel: rel1},
                    ProgNode::RelNode{rel: rel2},
                    ProgNode::RelNode{rel: rel3}]
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

    assert_eq!(running.relation_clone_content(3).unwrap(), set);

    running.stop().unwrap();

}

#[test]
fn test_join_1() {
    test_join(1)
}

/*
#[test]
fn test_join_16() {
    test_join(16)
}*/

/* Antijoin
 */

#[cfg(test)]
fn test_antijoin(nthreads: usize) {
    let rel1 = Relation {
        name:         "T1".to_string(),
        input:        true,
        id:           1,      
        rules:        Vec::new(),
        arrangements: Vec::new()
    };
    fn afun1(v: Value) -> Option<(Value, Value)> {
        match &v {
            Value::Tuple2(v1,_) => Some(((**v1).clone(), v.clone())),
            _ => None
        }
    }
    let rel2 = Relation {
        name:         "T2".to_string(),
        input:        true,
        id:           2,      
        rules:        Vec::new(),
        arrangements: vec![Arrangement{
            name: "arrange2.0".to_string(),
            afun: &(afun1 as ArrangeFunc<Value>)
        }]
    };

    let rel3 = Relation {
        name:         "T3".to_string(),
        input:        false,
        id:           3,
        rules:        vec![Rule{
            rel: 1, 
            xforms: vec![XForm::Antijoin{
               afun:        &(afun1 as ArrangeFunc<Value>),
               arrangement: (2,0)
            }]
        }],
        arrangements: Vec::new()
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::RelNode{rel: rel1},
                    ProgNode::RelNode{rel: rel2},
                    ProgNode::RelNode{rel: rel3}]
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

    assert_eq!(running.relation_clone_content(3).unwrap().len(), 0);

    /* 1. T2 is empty; antijoin is identical to T1 */
    running.transaction_start().unwrap();
    for x in &set {
        running.delete(2, x.clone()).unwrap();
    };
    running.transaction_commit().unwrap();

    assert_eq!(running.relation_clone_content(3).unwrap(), set);

    running.stop().unwrap();
}

#[test]
fn test_antijoin_1() {
    test_antijoin(1)
}

/*
#[test]
fn test_antijoin_16() {
    test_antijoin(16)
}*/

/* Maps and filters
 */
#[cfg(test)]
fn test_map(nthreads: usize) {
    let rel1 = Relation {
        name:         "T1".to_string(),
        input:        true,
        id:           1,
        rules:        Vec::new(),
        arrangements: Vec::new()
    };

    fn mfun(v: Value) -> Value {
        match &v {
            Value::u64(uv) => Value::u64(uv * 2), 
            _ => v.clone()
        }
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

    let rel2 = Relation {
        name:         "T2".to_string(),
        input:        false,
        id:           2,      
        rules:        vec![Rule{
            rel: 1,
            xforms: vec![
                XForm::Map{
                    mfun: &(mfun as MapFunc<Value>)
                },
                XForm::Filter{
                    ffun: &(ffun as FilterFunc<Value>)
                },
                XForm::FilterMap{
                    fmfun: &(fmfun as FilterMapFunc<Value>)
                }]
        }],
        arrangements: Vec::new()
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::RelNode{rel: rel1},
                    ProgNode::RelNode{rel: rel2}]
    };

    let mut running = prog.run(nthreads);

    let vals:Vec<u64> = (0..TEST_SIZE).collect();
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::u64(*x)));
    let expected = FnvHashSet::from_iter(vals.iter().
                                         map(|x| Value::u64(*x)).
                                         map(|x| mfun(x)).
                                         filter(|x| ffun(&x)).
                                         filter_map(|x| fmfun(x)));

    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
    };
    running.transaction_commit().unwrap();

    assert_eq!(running.relation_clone_content(1).unwrap(), set);
    assert_eq!(running.relation_clone_content(2).unwrap(), expected);
    running.stop().unwrap();
}

#[test]
fn test_map_1() {
    test_map(1)
}

/*
#[test]
fn test_map_16() {
    test_map(16)
}*/


/* Recursion
 */
#[cfg(test)]
fn test_recursion(nthreads: usize) {
    fn arrange_by_parent(v: Value) -> Option<(Value, Value)> {
        match &v {
            Value::Tuple2(parent,child) => Some(((**parent).clone(), (**child).clone())),
            _ => None
        }
    }
    fn arrange_by_descendant(v: Value) -> Option<(Value, Value)> {
        match &v {
            Value::Tuple2(ancestor,descendant) => Some(((**descendant).clone(), (**ancestor).clone())),
            _ => None
        }
    }

    fn jfun(_parent: &Value, ancestor: &Value, child: &Value) -> Option<Value> {
        Some(Value::Tuple2(Box::new(ancestor.clone()), Box::new(child.clone())))
    }

    let parent = Relation {
        name:         "parent".to_string(),
        input:        true,
        id:           1,      
        rules:        Vec::new(),
        arrangements: vec![Arrangement{
            name: "arrange_by_parent".to_string(),
            afun: &(arrange_by_parent as ArrangeFunc<Value>)
        }]
    };

    let ancestor = Relation {
        name:         "ancestor".to_string(),
        input:        false,
        id:           2,      
        rules:        vec![
            Rule{
                rel: 1, 
                xforms: vec![]
            },
            Rule{
                rel: 2, 
                xforms: vec![XForm::Join{
                    afun:        &(arrange_by_descendant as ArrangeFunc<Value>),
                    arrangement: (1,0),
                    jfun:        &(jfun as JoinFunc<Value>)
                }]
            }],
        arrangements: Vec::new()
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::RelNode{rel: parent},
                    ProgNode::SCCNode{rels: vec![ancestor]}]
    };

    let mut running = prog.run(nthreads);

    /* 1. Populate parent relation */
    let vals = vec![Value::Tuple2(Box::new(Value::String("A".to_string())), Box::new(Value::String("B".to_string()))),
                    Value::Tuple2(Box::new(Value::String("B".to_string())), Box::new(Value::String("C".to_string())))];
    let set = FnvHashSet::from_iter(vals.iter().map(|x| x.clone()));

    let expect_vals = vec![Value::Tuple2(Box::new(Value::String("A".to_string())), Box::new(Value::String("B".to_string()))),
                           Value::Tuple2(Box::new(Value::String("B".to_string())), Box::new(Value::String("C".to_string()))),
                           Value::Tuple2(Box::new(Value::String("A".to_string())), Box::new(Value::String("C".to_string())))];
    
    let expect_set = FnvHashSet::from_iter(expect_vals.iter().map(|x| x.clone()));

    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
    };
    running.transaction_commit().unwrap();

    assert_eq!(running.relation_clone_content(1).unwrap(), set);
    assert_eq!(running.relation_clone_content(2).unwrap(), expect_set);


    /* 2. Remove record from "parent" relation */
    running.transaction_start().unwrap();
    running.delete(1, vals[0].clone()).unwrap();
    println!("delta1: {:?}", running.relation_clone_delta(1).unwrap());
    println!("delta2: {:?}", running.relation_clone_delta(2).unwrap());
    running.transaction_commit().unwrap();

    let expect_vals2 = vec![Value::Tuple2(Box::new(Value::String("B".to_string())), Box::new(Value::String("C".to_string())))];
    let expect_set2 = FnvHashSet::from_iter(expect_vals2.iter().map(|x| x.clone()));

    assert_eq!(running.relation_clone_content(2).unwrap(), expect_set2);

    running.stop().unwrap();
}


#[test]
fn test_recursion_1() {
    test_recursion(1)
}

/*
#[test]
fn test_recursion_16() {
    test_recursion(16)
}*/
