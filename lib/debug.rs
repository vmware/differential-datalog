use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::string::ToString;

pub fn debug_debug_event<T1: ToString, A1: fmt::Debug, A2: fmt::Debug>(
    operator_id: &(u32, u32, u32),
    w: &std_DDWeight,
    ts: &T1,
    input1: &A1,
    out: &A2,
) {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("debug.log".to_string())
        .unwrap();
    write!(
        &file,
        "{:?}, {}, {}, {:?}, {:?}\n",
        &operator_id,
        &w.to_string(),
        &ts.to_string(),
        &input1,
        &out
    );
    ()
}

pub fn debug_debug_event_join<T1: ToString, A1: fmt::Debug, A2: fmt::Debug, A3: fmt::Debug>(
    operator_id: &(u32, u32, u32),
    w: &std_DDWeight,
    ts: &T1,
    input1: &A1,
    input2: &A2,
    out: &A3,
) {
    let mut file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("debug.log".to_string())
        .unwrap();
    write!(
        &file,
        "{:?}, {}, {}, {:?}, {:?}, {:?}\n",
        &operator_id,
        &w.to_string(),
        &ts.to_string(),
        &input1,
        &input2,
        &out
    );
    ()
}


pub fn debug_debug_split_group<'a, K, I: 'static + Clone, V: 'static>(g: &'a std_Group<'a, K, (I,V)>) -> (std_Vec<I>, std_Group<'a, K, V>) {
    let mut inputs = std_Vec::with_capacity(std_group_count(g) as usize);
    for (i, _) in g.iter() {
        inputs.push(i.clone())
    };
    let orig_project = g.project.clone();
    (inputs, std_Group::new(g.key, g.group, std::rc::Rc::new(move |v| (orig_project)(v).1)))
}
