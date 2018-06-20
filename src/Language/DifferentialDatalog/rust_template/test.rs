#![allow(non_camel_case_types)]

#[cfg(test)]

use program::*;
use uint::*;
use abomonation::Abomonation;
use fnv::FnvHashSet;
use std::iter::FromIterator;

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

impl Default for Value {
    fn default() -> Value {Value::bool(false)}
}

/* Test insertion/deletion into a database with a single table and no rules
 */
#[test]
fn test_one_relation() {
    let rel = Relation {
        name:         "T1".to_string(),
        id:           1,      
        rules:        Vec::new(),
        arrangements: Vec::new()
    };

    let prog: Program<Value> = Program {
        nodes: vec![ProgNode::RelNode{rel}]
    };

    let mut running = prog.run(1);

    /* 1. Insertion */
    let vals:Vec<u64> = (0..10).collect();
    let mut set = FnvHashSet::from_iter(vals.iter().map(|x| Value::u64(*x)));

    for x in &set {
        running.transaction_start().unwrap();
        running.insert(1, x.clone()).unwrap();
        running.transaction_commit().unwrap();
    };

    assert_eq!(*running.relation_content(1).unwrap().lock().unwrap(), set);

    /* 2. Deletion */
    let mut set2 = set.clone();
    running.transaction_start().unwrap();
    for x in set.drain() {
        set2.remove(&x);
        running.delete(1, x.clone()).unwrap();
        assert_eq!(*running.relation_content(1).unwrap().lock().unwrap(), set2);
    };
    running.transaction_commit().unwrap();
    assert_eq!(running.relation_content(1).unwrap().lock().unwrap().len(), 0);

    /* 3. Test set semantics: insert twice, delete once */
    running.transaction_start().unwrap();
    running.insert(1, Value::u64(1)).unwrap();
    running.insert(1, Value::u64(1)).unwrap();
    running.delete(1, Value::u64(1)).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(running.relation_content(1).unwrap().lock().unwrap().len(), 0);

    /* 4. Set semantics: delete before insert */
    running.transaction_start().unwrap();
    running.delete(1, Value::u64(1)).unwrap();
    running.insert(1, Value::u64(1)).unwrap();
    running.transaction_commit().unwrap();

    assert_eq!(running.relation_content(1).unwrap().lock().unwrap().len(), 1);

    /* 5. Rollback */
    let set = FnvHashSet::from_iter(vals.iter().map(|x| Value::u64(*x)));

    let before = running.relation_content(1).unwrap().lock().unwrap().clone();
    running.transaction_start().unwrap();
    for x in &set {
        running.insert(1, x.clone()).unwrap();
    };
    //println!("delta: {:?}", *running.relation_delta(1).unwrap().lock().unwrap());
    assert_eq!(*running.relation_content(1).unwrap().lock().unwrap(), set);
    assert_eq!(running.relation_delta(1).unwrap().lock().unwrap().len(), set.len() - 1);
    running.transaction_rollback().unwrap();

    assert_eq!(*running.relation_content(1).unwrap().lock().unwrap(), before);

    running.stop().unwrap();
}


/*
#[test]
fn test_simple_prog() {
    let prog: Program<Value> = Program {
        nodes: Vec::new()
    };
    let mut running = prog.run(16);
    for _i in 0..10 {
        running.transaction_start().unwrap();
        running.insert(0, Value::bool(true)).unwrap();
        running.transaction_commit().unwrap();
    };
    running.stop().unwrap();
}*/
