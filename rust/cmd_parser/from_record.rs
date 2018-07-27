//! `FromRecord` trait.  For types that 

use parse::Record;
use num::{ToPrimitive, BigInt, BigUint};

#[cfg(test)]
use num::bigint::{ToBigInt, ToBigUint};

pub trait FromRecord: Sized {
    fn from_record(val: &Record) -> Result<Self, String>;
}

impl FromRecord for u8 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_u8() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot conver {} to u8", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
        }
    }
}


#[test]
fn test_u8() {
    assert_eq!(u8::from_record(&Record::Int(25_u8.to_bigint().unwrap())), Ok(25));
    assert_eq!(u8::from_record(&Record::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u8::from_record(&Record::Int(0xabcd.to_bigint().unwrap())), Err("cannot conver 43981 to u8".to_string()));
}


impl FromRecord for u16 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_u16() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot conver {} to u16", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
        }
    }
}

#[test]
fn test_u16() {
    assert_eq!(u16::from_record(&Record::Int(25_u16.to_bigint().unwrap())), Ok(25));
    assert_eq!(u16::from_record(&Record::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u16::from_record(&Record::Int(0xabcdef.to_bigint().unwrap())), Err("cannot conver 11259375 to u16".to_string()));
}


impl FromRecord for u32 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_u32() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot conver {} to u32", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
        }
    }
}

#[test]
fn test_u32() {
    assert_eq!(u32::from_record(&Record::Int(25_u32.to_bigint().unwrap())), Ok(25));
    assert_eq!(u32::from_record(&Record::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u32::from_record(&Record::Int(0xabcdef.to_bigint().unwrap())), Ok(0xabcdef));
    assert_eq!(u32::from_record(&Record::Int((-0xabcdef).to_bigint().unwrap())), Err("cannot conver -11259375 to u32".to_string()));
}


impl FromRecord for u64 {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_u64() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot conver {} to u64", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
        }
    }
}

#[test]
fn test_u64() {
    assert_eq!(u64::from_record(&Record::Int(25_u64.to_bigint().unwrap())), Ok(25));
    assert_eq!(u64::from_record(&Record::Int(0xab.to_bigint().unwrap())), Ok(0xab));
    assert_eq!(u64::from_record(&Record::Int(0xabcdef.to_bigint().unwrap())), Ok(0xabcdef));
    assert_eq!(u64::from_record(&Record::Int((-0xabcdef).to_bigint().unwrap())), Err("cannot conver -11259375 to u64".to_string()));
}

impl FromRecord for BigInt {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => Result::Ok(i.clone()),
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
        }
    }
}

#[test]
fn test_bigint() {
    let v = (-25_i64).to_bigint().unwrap();
    assert_eq!(BigInt::from_record(&Record::Int(v.clone())), Ok(v));
}

impl FromRecord for BigUint {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Int(i) => {
                match i.to_biguint() {
                    Some(x) => Result::Ok(x),
                    None    => Result::Err(format!("cannot conver {} to BigUint", i))
                }
            },
            v => {
                Result::Err(format!("not an int {:?}", *v))
            }
        }
    }
}

#[test]
fn test_biguint() {
    let vi = (25_i64).to_bigint().unwrap();
    let vu = (25_i64).to_biguint().unwrap();
    assert_eq!(BigUint::from_record(&Record::Int(vi)), Ok(vu));

    let vi = (-25_i64).to_bigint().unwrap();
    assert_eq!(BigUint::from_record(&Record::Int(vi)), Err("cannot conver -25 to BigUint".to_string()));
}


impl FromRecord for bool {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Bool(b) => { Result::Ok(*b) },
            v => {
                Result::Err(format!("not a bool {:?}", *v))
            }
        }
    }
}

#[test]
fn test_bool() {
    assert_eq!(bool::from_record(&Record::Bool(true)), Ok(true));
}


impl FromRecord for String {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::String(s) => { Result::Ok(s.clone()) },
            v => {
                Result::Err(format!("not a string {:?}", *v))
            }
        }
    }
}

#[test]
fn test_string() {
    assert_eq!(String::from_record(&Record::String("foo".to_string())), Ok("foo".to_string()));
    assert_eq!(String::from_record(&Record::Bool(true)), Err("not a string Bool(true)".to_string()));
}

impl <T1: FromRecord, T2: FromRecord> FromRecord for (T1,T2) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 2 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?))
            },
            v => { Result::Err(format!("not a 2-tuple {:?}", *v)) }
        }
    }
}

impl <T1: FromRecord, T2: FromRecord, T3: FromRecord> FromRecord for (T1,T2,T3) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 3 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?,
                    T3::from_record(&args[2])?))
            },
            v => { Result::Err(format!("not a 3-tuple {:?}", *v)) }
        }
    }
}

impl <T1: FromRecord, T2: FromRecord, T3: FromRecord, T4: FromRecord> FromRecord for (T1,T2,T3,T4) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 4 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?,
                    T3::from_record(&args[2])?,
                    T4::from_record(&args[3])?))
            },
            v => { Result::Err(format!("not a 4-tuple {:?}", *v)) }
        }
    }
}

impl <T1: FromRecord, T2: FromRecord, T3: FromRecord, T4: FromRecord, T5: FromRecord> FromRecord for (T1,T2,T3,T4,T5) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 5 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?,
                    T3::from_record(&args[2])?,
                    T4::from_record(&args[3])?,
                    T5::from_record(&args[4])?))
            },
            v => { Result::Err(format!("not a 5-tuple {:?}", *v)) }
        }
    }
}

impl <T1: FromRecord, T2: FromRecord, T3: FromRecord, T4: FromRecord, T5: FromRecord, T6: FromRecord> FromRecord for (T1,T2,T3,T4,T5,T6) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 6 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?,
                    T3::from_record(&args[2])?,
                    T4::from_record(&args[3])?,
                    T5::from_record(&args[4])?,
                    T6::from_record(&args[5])?))
            },
            v => { Result::Err(format!("not a 6-tuple {:?}", *v)) }
        }
    }
}

impl <T1: FromRecord, T2: FromRecord, T3: FromRecord, T4: FromRecord, T5: FromRecord, T6: FromRecord, T7: FromRecord> FromRecord for (T1,T2,T3,T4,T5,T6,T7) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 7 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?,
                    T3::from_record(&args[2])?,
                    T4::from_record(&args[3])?,
                    T5::from_record(&args[4])?,
                    T6::from_record(&args[5])?,
                    T7::from_record(&args[6])?))
            },
            v => { Result::Err(format!("not a 7-tuple {:?}", *v)) }
        }
    }
}

impl <T1: FromRecord, T2: FromRecord, T3: FromRecord, T4: FromRecord, T5: FromRecord, T6: FromRecord, T7: FromRecord, T8: FromRecord> FromRecord for (T1,T2,T3,T4,T5,T6,T7,T8) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 8 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?,
                    T3::from_record(&args[2])?,
                    T4::from_record(&args[3])?,
                    T5::from_record(&args[4])?,
                    T6::from_record(&args[5])?,
                    T7::from_record(&args[6])?,
                    T8::from_record(&args[7])?))
            },
            v => { Result::Err(format!("not a 8-tuple {:?}", *v)) }
        }
    }
}

impl <T1: FromRecord, T2: FromRecord, T3: FromRecord, T4: FromRecord, T5: FromRecord, T6: FromRecord, T7: FromRecord, T8: FromRecord, T9: FromRecord> FromRecord for (T1,T2,T3,T4,T5,T6,T7,T8,T9) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 9 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?,
                    T3::from_record(&args[2])?,
                    T4::from_record(&args[3])?,
                    T5::from_record(&args[4])?,
                    T6::from_record(&args[5])?,
                    T7::from_record(&args[6])?,
                    T8::from_record(&args[7])?,
                    T9::from_record(&args[8])?))
            },
            v => { Result::Err(format!("not a 9-tuple {:?}", *v)) }
        }
    }
}

impl <T1: FromRecord, T2: FromRecord, T3: FromRecord, T4: FromRecord, T5: FromRecord, T6: FromRecord, T7: FromRecord, T8: FromRecord, T9: FromRecord, T10: FromRecord> FromRecord for (T1,T2,T3,T4,T5,T6,T7,T8,T9,T10) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 10 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?,
                    T3::from_record(&args[2])?,
                    T4::from_record(&args[3])?,
                    T5::from_record(&args[4])?,
                    T6::from_record(&args[5])?,
                    T7::from_record(&args[6])?,
                    T8::from_record(&args[7])?,
                    T9::from_record(&args[8])?,
                    T10::from_record(&args[9])?))
            },
            v => { Result::Err(format!("not a 10-tuple {:?}", *v)) }
        }
    }
}

impl <T1: FromRecord, T2: FromRecord, T3: FromRecord, T4: FromRecord, T5: FromRecord, T6: FromRecord, T7: FromRecord, T8: FromRecord, T9: FromRecord, T10: FromRecord, T11: FromRecord> FromRecord for (T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 11 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?,
                    T3::from_record(&args[2])?,
                    T4::from_record(&args[3])?,
                    T5::from_record(&args[4])?,
                    T6::from_record(&args[5])?,
                    T7::from_record(&args[6])?,
                    T8::from_record(&args[7])?,
                    T9::from_record(&args[8])?,
                    T10::from_record(&args[9])?,
                    T11::from_record(&args[10])?))
            },
            v => { Result::Err(format!("not a 11-tuple {:?}", *v)) }
        }
    }
}

impl <T1: FromRecord, T2: FromRecord, T3: FromRecord, T4: FromRecord, T5: FromRecord, T6: FromRecord, T7: FromRecord, T8: FromRecord, T9: FromRecord, T10: FromRecord, T11: FromRecord, T12: FromRecord> FromRecord for (T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12) {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Tuple(args) if args.len() == 12 => {
                Ok((T1::from_record(&args[0])?, 
                    T2::from_record(&args[1])?,
                    T3::from_record(&args[2])?,
                    T4::from_record(&args[3])?,
                    T5::from_record(&args[4])?,
                    T6::from_record(&args[5])?,
                    T7::from_record(&args[6])?,
                    T8::from_record(&args[7])?,
                    T9::from_record(&args[8])?,
                    T10::from_record(&args[9])?,
                    T11::from_record(&args[10])?,
                    T12::from_record(&args[11])?))
            },
            v => { Result::Err(format!("not a 12-tuple {:?}", *v)) }
        }
    }
}

#[test]
fn test_tuple() {
    assert_eq!(<(bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false)])), 
               Ok((true, false)));
    assert_eq!(<(bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false)])), 
               Ok((true, false, false)));
    assert_eq!(<(bool, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true)])), 
               Ok((true, false, false, true)));
    assert_eq!(<(bool, bool, bool, bool, String)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string())])), 
               Ok((true, false, false, true, "foo".to_string())));
    assert_eq!(<(bool, bool, bool, bool, String, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false)])), 
               Ok((true, false, false, true, "foo".to_string(), false)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false)])), 
               Ok((true, false, false, true, "foo".to_string(), false, false)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false), Record::Bool(false)])), 
               Ok((true, false, false, true, "foo".to_string(), false, false, false)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false), Record::Bool(false), Record::Bool(true)])), 
               Ok((true, false, false, true, "foo".to_string(), false, false, false, true)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::Bool(true)])), 
               Ok((true, false, false, true, "foo".to_string(), false, false, false, true, true)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool, bool, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::Bool(true), Record::Bool(true)])), 
               Ok((true, false, false, true, "foo".to_string(), false, false, false, true, true, true)));
    assert_eq!(<(bool, bool, bool, bool, String, bool, bool, bool, bool, bool, bool, bool)>::from_record(&Record::Tuple(vec![Record::Bool(true), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::String("foo".to_string()), Record::Bool(false), Record::Bool(false), Record::Bool(false), Record::Bool(true), Record::Bool(true), Record::Bool(true), Record::Bool(false)])), 
               Ok((true, false, false, true, "foo".to_string(), false, false, false, true, true, true, false)));
}

#[cfg(test)]
#[derive(Eq, PartialEq, Debug)]
struct Foo<T> {
    f1: T
}

#[cfg(test)]
impl <T: FromRecord> FromRecord for Foo<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Struct(constr, args) => {
                match constr.as_ref() {
                    "Foo" if args.len() == 1 => {
                        Ok(Foo{f1: T::from_record(&args[0])?})
                    },
                    c => Result::Err(format!("unknown constructor {} of type Foo in {:?}", c, *val))
                }
            },
            v => {
                Result::Err(format!("not a struct {:?}", *v))
            }
        }
    }
}

#[cfg(test)]
type Bbool = bool;

#[cfg(test)]
#[derive(Eq, PartialEq, Debug)]
enum DummyEnum<T> {
    Constr1{f1: Bbool, f2: String},
    Constr2{f1: T, f2: BigInt, f3: Foo<T>},
    Constr3{f1: (bool, bool)},
}

#[cfg(test)]
impl <T: FromRecord> FromRecord for DummyEnum<T> {
    fn from_record(val: &Record) -> Result<Self, String> {
        match val {
            Record::Struct(constr, args) => {
                match constr.as_ref() {
                    "Constr1" if args.len() == 2 => {
                        Ok(DummyEnum::Constr1{f1: <Bbool>::from_record(&args[0])?,
                                              f2: String::from_record(&args[1])?})
                    },
                    "Constr2" if args.len() == 3 => {
                        Ok(DummyEnum::Constr2{f1: <T>::from_record(&args[0])?,
                                              f2: <BigInt>::from_record(&args[1])?,
                                              f3: <Foo<T>>::from_record(&args[2])?})
                    },
                    "Constr3" if args.len() == 1 => {
                        Ok(DummyEnum::Constr3{f1: <(bool,bool)>::from_record(&args[0])?})
                    },
                    c => Result::Err(format!("unknown constructor {} of type DummyEnum in {:?}", c, *val))
                }
            },
            v => {
                Result::Err(format!("not a struct {:?}", *v))
            }
        }
    }
}


#[test]
fn test_enum() {
    assert_eq!(DummyEnum::from_record(&Record::Struct("Constr1".to_string(),
                                                    vec![Record::Bool(true), Record::String("foo".to_string())])),
               Ok(DummyEnum::Constr1::<bool>{f1: true, f2: "foo".to_string()}));
    assert_eq!(DummyEnum::from_record(&Record::Struct("Constr2".to_string(),
                                                    vec![Record::Int((5_i64).to_bigint().unwrap()), 
                                                         Record::Int((25_i64).to_bigint().unwrap()), 
                                                         Record::Struct("Foo".to_string(), vec![Record::Int((0_i64).to_bigint().unwrap())])])),
               Ok(DummyEnum::Constr2::<u16>{f1: 5, 
                                            f2: (25_i64).to_bigint().unwrap(),
                                            f3: Foo{f1: 0}}));
}
