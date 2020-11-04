use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::string::ToString;

pub fn debug_event<T1: ToString, A1: Clone + IntoRecord, A2: Clone + IntoRecord>(
    operator_id: &ddlog_std::tuple3<u32, u32, u32>,
    w: &ddlog_std::DDWeight,
    ts: &T1,
    operator_type: &String,
    input1: &A1,
    out: &A2,
) {
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("debug.log".to_string())
        .unwrap();

    let _ = writeln!(
        &file,
        "({},{},{}), {}, {}, {}, {}, {}",
        &operator_id.0,
        &operator_id.1,
        &operator_id.2,
        &w.to_string(),
        &ts.to_string(),
        &operator_type,
        &input1.clone().into_record(),
        &out.clone().into_record()
    );
}

pub fn debug_event_join<
    T1: ToString,
    A1: Clone + IntoRecord,
    A2: Clone + IntoRecord,
    A3: Clone + IntoRecord,
>(
    operator_id: &ddlog_std::tuple3<u32, u32, u32>,
    w: &ddlog_std::DDWeight,
    ts: &T1,
    input1: &A1,
    input2: &A2,
    out: &A3,
) {
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("debug.log".to_string())
        .unwrap();

    let _ = writeln!(
        &file,
        "({},{},{}), {}, {}, Join, {}, {}, {}",
        &operator_id.0,
        &operator_id.1,
        &operator_id.2,
        &w.to_string(),
        &ts.to_string(),
        &input1.clone().into_record(),
        &input2.clone().into_record(),
        &out.clone().into_record()
    );
}

pub fn debug_split_group<K: Clone, I: 'static + Clone, V: Clone + 'static>(
    g: &ddlog_std::Group<K, ddlog_std::tuple2<I, V>>,
) -> ddlog_std::tuple2<ddlog_std::Vec<I>, ddlog_std::Group<K, V>> {
    let mut inputs = ddlog_std::Vec::with_capacity(ddlog_std::group_count(g) as usize);
    let mut vals = ::std::vec::Vec::with_capacity(ddlog_std::group_count(g) as usize);
    for ddlog_std::tuple2(i, v) in g.iter() {
        inputs.push(i);
        vals.push(v);
    }

    ddlog_std::tuple2(inputs, ddlog_std::Group::new(g.key(), vals))
}
