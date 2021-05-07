use std::fmt::Debug;

pub fn print(msg: &String) {
    print!("{}\n", msg)
}

pub fn debug<T: std::fmt::Debug>(value: &T) {
    println!("{:?}", value);
}
