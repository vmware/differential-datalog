use differential_datalog::record;
use differential_datalog::record::Record;
use std::cmp;
use std::fmt;
use std::hash::Hash;

#[derive(Default, Eq, PartialEq, Clone, Hash)]
pub struct Intern<A>
where
    A: Eq + Send + Sync + Hash + 'static,
{
    intern: arc_interner::ArcIntern<A>,
}

impl<A: Eq + Send + Sync + Hash + 'static> PartialOrd for Intern<A> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        let sptr = self.as_ref() as *const A as usize;
        let optr = other.as_ref() as *const A as usize;

        sptr.partial_cmp(&optr)
    }
}

impl<A: Eq + Send + Sync + Hash + 'static> Ord for Intern<A> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let sptr = self.as_ref() as *const A as usize;
        let optr = other.as_ref() as *const A as usize;

        sptr.cmp(&optr)
    }
}

impl<A: Eq + Send + Sync + Hash + 'static> Deref for Intern<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        self.intern.deref()
    }
}

impl<A: Eq + Hash + Send + Sync + 'static> Intern<A> {
    pub fn new(x: A) -> Intern<A> {
        Intern {
            intern: arc_interner::ArcIntern::new(x),
        }
    }
}

impl<A> AsRef<A> for Intern<A>
where
    A: Eq + Hash + Send + Sync + 'static,
{
    fn as_ref(&self) -> &A {
        self.intern.as_ref()
    }
}

pub fn intern<A: Eq + Hash + Send + Sync + Clone + 'static>(x: &A) -> Intern<A> {
    Intern::new(x.clone())
}

pub fn ival<A: Eq + Hash + Send + Sync + Clone>(x: &Intern<A>) -> &A {
    x.intern.as_ref()
}

/*pub fn intern_istring_ord(s: &intern_istring) -> u32 {
    s.x
}*/

impl<A: fmt::Display + Eq + Hash + Send + Sync + Clone> fmt::Display for Intern<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self.as_ref(), f)
        //record::format_ddlog_str(&intern_istring_str(self), f)
    }
}

impl<A: fmt::Debug + Eq + Hash + Send + Sync + Clone> fmt::Debug for Intern<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self.as_ref(), f)
        //record::format_ddlog_str(&intern_istring_str(self), f)
    }
}

impl<A: Serialize + Eq + Hash + Send + Sync + Clone> serde::Serialize for Intern<A> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.as_ref().serialize(serializer)
    }
}

impl<'de, A: Deserialize<'de> + Eq + Hash + Send + Sync + 'static> serde::Deserialize<'de>
    for Intern<A>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        A::deserialize(deserializer).map(Intern::new)
    }
}

impl<A: FromRecord + Eq + Hash + Send + Sync + 'static> FromRecord for Intern<A> {
    fn from_record(val: &Record) -> Result<Self, String> {
        A::from_record(val).map(Intern::new)
    }
}

impl<A: IntoRecord + Eq + Hash + Send + Sync + Clone> IntoRecord for Intern<A> {
    fn into_record(self) -> Record {
        ival(&self).clone().into_record()
    }
}

impl<A> Mutator<Intern<A>> for Record
where
    A: Clone + Eq + Send + Sync + Hash,
    Record: Mutator<A>,
{
    fn mutate(&self, x: &mut Intern<A>) -> Result<(), String> {
        let mut v = ival(x).clone();
        self.mutate(&mut v)?;
        *x = intern(&v);
        Ok(())
    }
}

pub fn istring_join(strings: &ddlog_std::Vec<istring>, sep: &String) -> String {
    strings
        .x
        .iter()
        .map(|s| s.as_ref())
        .cloned()
        .collect::<Vec<String>>()
        .join(sep.as_str())
}

pub fn istring_split(s: &istring, sep: &String) -> ddlog_std::Vec<String> {
    ddlog_std::Vec {
        x: s.as_ref().split(sep).map(|x| x.to_owned()).collect(),
    }
}

pub fn istring_contains(s1: &istring, s2: &String) -> bool {
    s1.as_ref().contains(s2.as_str())
}

pub fn istring_substr(s: &istring, start: &std_usize, end: &std_usize) -> String {
    let len = s.as_ref().len();
    let from = cmp::min(*start as usize, len);
    let to = cmp::max(from, cmp::min(*end as usize, len));
    s.as_ref()[from..to].to_string()
}

pub fn istring_replace(s: &istring, from: &String, to: &String) -> String {
    s.as_ref().replace(from, to)
}

pub fn istring_starts_with(s: &istring, prefix: &String) -> bool {
    s.as_ref().starts_with(prefix)
}

pub fn istring_ends_with(s: &istring, suffix: &String) -> bool {
    s.as_ref().ends_with(suffix)
}

pub fn istring_trim(s: &istring) -> String {
    s.as_ref().trim().to_string()
}

pub fn istring_len(s: &istring) -> std_usize {
    s.as_ref().len() as std_usize
}

pub fn istring_to_bytes(s: &istring) -> ddlog_std::Vec<u8> {
    ddlog_std::Vec::from(s.as_ref().as_bytes())
}

pub fn istring_to_lowercase(s: &istring) -> String {
    s.as_ref().to_lowercase()
}

pub fn istring_to_uppercase(s: &istring) -> String {
    s.as_ref().to_uppercase()
}

pub fn istring_reverse(s: &istring) -> String {
    s.as_ref().chars().rev().collect()
}
