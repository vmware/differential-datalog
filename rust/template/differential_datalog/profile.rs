//! Memory profile of a DDlog program.

use differential_dataflow::logging::DifferentialEvent;
use fnv::FnvHashMap;
use sequence_trie::SequenceTrie;
use std::cell::RefCell;
use std::cmp::max;
use std::fmt;
use std::time::Duration;
use timely::logging::{OperatesEvent, ScheduleEvent, StartStop, TimelyEvent};

thread_local! {
    pub static PROF_CONTEXT: RefCell<String> = RefCell::new("".to_string());
}

pub fn set_prof_context(s: &str) {
    PROF_CONTEXT.with(|ctx| *ctx.borrow_mut() = s.to_string());
}

pub fn get_prof_context() -> String {
    PROF_CONTEXT.with(|ctx| ctx.borrow().to_string())
}

pub fn with_prof_context<T, F: FnOnce() -> T>(s: &str, f: F) -> T {
    set_prof_context(s);
    let res = f();
    set_prof_context("");
    res
}

/* Profiling information message sent by worker to profiling thread
 */
#[derive(Debug)]
pub enum ProfMsg {
    TimelyMessage(Vec<((Duration, usize, TimelyEvent), String)>),
    DifferentialMessage(Vec<(Duration, usize, DifferentialEvent)>),
}

#[derive(Clone, Debug)]
pub struct Profile {
    addresses: SequenceTrie<usize, usize>,
    names: FnvHashMap<usize, String>,
    sizes: FnvHashMap<usize, isize>,
    peak_sizes: FnvHashMap<usize, isize>,
    starts: FnvHashMap<(usize, usize), Duration>,
    durations: FnvHashMap<usize, (Duration, usize)>,
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "\nArrangement size profile\n")?;
        self.fmt_sizes(&self.sizes, f)?;

        write!(f, "\nArrangement peak sizes\n")?;
        self.fmt_sizes(&self.peak_sizes, f)?;

        write!(f, "\nCPU profile\n")?;
        self.fmt_durations(0, &self.addresses, f)?;

        Ok(())
    }
}

impl Profile {
    pub fn new() -> Profile {
        Profile {
            addresses: SequenceTrie::new(),
            names: FnvHashMap::default(),
            sizes: FnvHashMap::default(),
            peak_sizes: FnvHashMap::default(),
            starts: FnvHashMap::default(),
            durations: FnvHashMap::default(),
        }
    }

    pub fn fmt_sizes(
        &self,
        sizes: &FnvHashMap<usize, isize>,
        f: &mut fmt::Formatter,
    ) -> Result<(), fmt::Error> {
        let mut size_vec: Vec<(usize, isize)> = sizes.clone().into_iter().collect();
        size_vec.sort_by(|a, b| a.1.cmp(&b.1).reverse());
        size_vec
            .iter()
            .map(|(operator, size)| {
                let name = self.names.get(operator).map(AsRef::as_ref).unwrap_or("???");
                let msg = format!("{} {}", name, operator);
                writeln!(f, "{}      {}", size, msg)
            })
            .collect()
    }

    pub fn fmt_durations(
        &self,
        depth: usize,
        addrs: &SequenceTrie<usize, usize>,
        f: &mut fmt::Formatter,
    ) -> Result<(), fmt::Error> {
        /* Sort children in the order of decreasing duration */
        let mut children = addrs.children();
        children.sort_by(|child1, child2| {
            let dur1 = child1
                .value()
                .map(|opid| self.durations.get(opid).cloned().unwrap_or_default().0)
                .unwrap_or_default();
            let dur2 = child2
                .value()
                .map(|opid| self.durations.get(opid).cloned().unwrap_or_default().0)
                .unwrap_or_default();
            dur1.cmp(&dur2).reverse()
        });

        for child in children.iter() {
            /* Print the duration of the child before calling the function recursively on it */
            match child.value() {
                None => {
                    writeln!(f, "Unknown operator")?;
                }
                Some(opid) => {
                    let name = self.names.get(opid).map(AsRef::as_ref).unwrap_or("???");
                    let duration = self.durations.get(opid).cloned().unwrap_or_default();
                    let msg = format!("{} {}", name, opid);
                    let offset = (0..depth * 2).map(|_| " ").collect::<String>();
                    writeln!(
                        f,
                        "{}{: >6}s{:0>6}us ({: >9}calls)     {}",
                        offset,
                        duration.0.as_secs(),
                        duration.0.subsec_micros(),
                        duration.1,
                        msg
                    )?;
                }
            }
            self.fmt_durations(depth + 1, child, f)?;
        }
        Ok(())
    }

    pub fn update(&mut self, msg: &ProfMsg) {
        match msg {
            ProfMsg::TimelyMessage(msg) => self.handle_timely(msg),
            ProfMsg::DifferentialMessage(msg) => self.handle_differential(msg),
        }
    }

    fn handle_timely(&mut self, msg: &[((Duration, usize, TimelyEvent), String)]) {
        for ((ts, worker, event), ctx) in msg.iter() {
            match event {
                TimelyEvent::Operates(OperatesEvent { id, addr, name }) => {
                    self.addresses.insert(addr, *id);
                    self.names.insert(*id, {
                        /* Remove redundant spaces. */
                        let frags: Vec<String> = (name.clone() + ": " + &ctx.replace('\n', " "))
                            .split_whitespace()
                            .map(|x| x.to_string())
                            .collect();
                        frags.join(" ")
                    });
                }
                TimelyEvent::Schedule(ScheduleEvent { id, start_stop }) => {
                    match start_stop {
                        StartStop::Start => {
                            self.starts.insert((*id, *worker), *ts);
                        }
                        StartStop::Stop => {
                            let (total, ncalls) = self
                                .durations
                                .entry(*id)
                                .or_insert((Duration::new(0, 0), 0));
                            let start = self.starts.get(&(*id,*worker)).cloned().unwrap_or_else(||{
                                eprintln!("TimelyEvent::Stop without a start for operator {}, worker {}", *id, *worker);
                                Duration::new(0,0)
                            });
                            *total += *ts - start;
                            *ncalls += 1;
                        }
                    }
                }
                _ => (),
            }
        }
    }

    fn handle_differential(&mut self, msg: &[(Duration, usize, DifferentialEvent)]) {
        //eprintln!("profiling message: {:?}", msg);
        for (_, _, event) in msg.iter() {
            match event {
                DifferentialEvent::Batch(x) => {
                    let size = self.sizes.entry(x.operator).or_insert(0);
                    *size += x.length as isize;
                    let peak = self.peak_sizes.entry(x.operator).or_insert(0);
                    *peak = max(*peak, *size);
                }
                DifferentialEvent::Merge(m) => {
                    if let Some(complete) = m.complete {
                        let size = self.sizes.entry(m.operator).or_insert(0);
                        *size += (complete as isize) - (m.length1 + m.length2) as isize;
                        let peak = self.peak_sizes.entry(m.operator).or_insert(0);
                        *peak = max(*peak, *size);
                    }
                }
                _ => (),
            }
        }
    }
}

impl Default for Profile {
    fn default() -> Self {
        Self::new()
    }
}
