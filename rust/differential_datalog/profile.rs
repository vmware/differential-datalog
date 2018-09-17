//! Memory profile of a DDlog program.

use fnv::FnvHashMap;
use std::time::Duration;
use std::fmt;
use std::cell::RefCell;
use std::cmp::max;
use differential_dataflow::logging::DifferentialEvent;
use timely::logging::TimelyEvent;
use timely::logging::OperatesEvent;

thread_local! {
    pub static PROF_CONTEXT: RefCell<String> = RefCell::new("".to_string());
}

pub fn set_prof_context(s: &str) {
    PROF_CONTEXT.with(|ctx| *ctx.borrow_mut() = s.to_string());
}

pub fn get_prof_context() -> String {
    PROF_CONTEXT.with(|ctx| ctx.borrow().to_string())
}

pub fn with_prof_context<T, F: FnOnce()->T>(s: &str, f: F) -> T {
    set_prof_context(s);
    let res = f();
    set_prof_context("");
    res
}


/* Profiling information message sent by worker to profiling thread
 */
pub enum ProfMsg {
    TimelyMessage(Vec<((Duration, usize, TimelyEvent), String)>),
    DifferentialMessage(Vec<(Duration, usize, DifferentialEvent)>)
}

#[derive(Clone)]
pub struct Profile {
    names: FnvHashMap<usize, String>,
    sizes: FnvHashMap<usize, isize>,
    peak_sizes: FnvHashMap<usize, isize>
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let res1: Result<(), fmt::Error> = self.sizes.iter().map(|(operator, size)| {
            let name = self.names.get(operator).map(|s|s.as_ref()).unwrap_or("???");
            write!(f, "current size of {} {}: {}\n", name, operator, size)
        }).collect();
        res1.and(
            self.peak_sizes.iter().map(|(operator, size)| {
                let name = self.names.get(operator).map(|s|s.as_ref()).unwrap_or("???");
                write!(f, "peak size of {} {}: {}\n", name, operator, size)
            }).collect())
    }
}

impl Profile {
    pub fn new() -> Profile {
        Profile{
            names:      FnvHashMap::default(),
            sizes:      FnvHashMap::default(),
            peak_sizes:  FnvHashMap::default()
        }
    }

    pub fn update(&mut self, msg: &ProfMsg) {
        match msg {
            ProfMsg::TimelyMessage(msg)       => self.handle_timely(msg),
            ProfMsg::DifferentialMessage(msg) => self.handle_differential(msg)
        }
    }

    fn handle_timely(&mut self, msg: &Vec<((Duration, usize, TimelyEvent), String)>) {
        for ((_, _, event), ctx) in msg.iter() {
            match event {
                TimelyEvent::Operates(OperatesEvent{id, addr:_, name}) => {
                    self.names.insert(*id, ctx.clone() + "." + name);
                },
                _ => ()
            }
        }
    }

    fn handle_differential(&mut self, msg: &Vec<(Duration, usize, DifferentialEvent)>) {
        //eprintln!("profiling message: {:?}", msg);
        for (_, _, event) in msg.iter() {
            match event {
                DifferentialEvent::Batch(x) => {
                    let size = self.sizes.entry(x.operator).or_insert(0);
                    *size += x.length as isize;
                    let peak = self.peak_sizes.entry(x.operator).or_insert(0);
                    *peak = max(*peak, *size);
                },
                DifferentialEvent::Merge(m) => {
                    if let Some(complete) = m.complete {
                        let size = self.sizes.entry(m.operator).or_insert(0);
                        *size += (complete as isize) - (m.length1 + m.length2) as isize;
                        let peak = self.peak_sizes.entry(m.operator).or_insert(0);
                        *peak = max(*peak, *size);
                    }
                },
                _ => (),
            }
        }
    }
}
