//! DDlog self-profiler.

mod debug_info;
mod profile_statistics;
mod source_code;

pub use debug_info::{ArrangementDebugInfo, OperatorDebugInfo, RuleDebugInfo};
use source_code::LinesIterator;
pub use source_code::{DDlogSourceCode, SourceFile, SourcePosition};

use profile_statistics::Statistics;

use differential_dataflow::logging::DifferentialEvent;
use fnv::FnvHashMap;
use once_cell::sync::Lazy;
use sequence_trie::SequenceTrie;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::{
    cell::RefCell,
    cmp::max,
    collections::{HashMap, HashSet},
    fs, include_str,
    iter::FromIterator,
    path::PathBuf,
    sync::Mutex,
    time::Duration,
    time::Instant,
};
use timely::logging::{OperatesEvent, ScheduleEvent, StartStop, TimelyEvent};

const PROFILER_UI_HTML: &str = include_str!("../profiler_ui/ui.html");
const PROFILER_UI_CSS: &str = include_str!("../profiler_ui/ui.css");
const PROFILER_UI_JS: &str = include_str!("../profiler_ui/ui.js");

// The first time the self-profiler generates a profile, it must create a
// directory that will store all profiles for the given DDlog instance, and
// dump program sources to this directory.  When profiling multiple DDlog
// instances within the same process, configured to use the same profiling
// directory, we only want to do this once per directory.  We use this static
// variable to track all existing profile directories.
static PROFILE_DIRECTORIES: Lazy<Mutex<HashSet<PathBuf>>> =
    Lazy::new(|| Mutex::new(HashSet::new()));

thread_local! {
    pub static PROF_CONTEXT: RefCell<Vec<OperatorDebugInfo>> = RefCell::new(vec![]);
}

pub fn push_prof_context(debug_info: OperatorDebugInfo) {
    PROF_CONTEXT.with(|ctx| ctx.borrow_mut().push(debug_info));
}

pub fn pop_prof_context() {
    PROF_CONTEXT.with(|ctx| {
        ctx.borrow_mut()
            .pop()
            .expect("'pop_prof_context' invoked with empty context stack")
    });
}

pub fn get_prof_context() -> Option<OperatorDebugInfo> {
    PROF_CONTEXT.with(|ctx| ctx.borrow().last().cloned())
}

pub fn with_prof_context<T, F: FnOnce() -> T>(debug_info: OperatorDebugInfo, f: F) -> T {
    push_prof_context(debug_info);
    let res = f();
    pop_prof_context();
    res
}

/// Profiling information message sent by worker to profiling thread
#[derive(Debug)]
pub enum ProfMsg {
    /// Send message batch as well as who the message is for (_, profile_cpu, profile_timely).
    TimelyMessage(
        Vec<((Duration, usize, TimelyEvent), Option<OperatorDebugInfo>)>,
        bool,
        bool,
    ),
    /// Message batch + change_profiling_enabled flag.
    DifferentialMessage(Vec<(Duration, usize, DifferentialEvent)>, bool),
}

/// Serialized representation of a CPU profile record.
#[derive(Serialize)]
pub struct CpuProfileRecord {
    opid: usize,
    cpu_us: u64,
    invocations: usize,
    short_descr: String,
    locations: Vec<SourcePosition>,
    descr: String,
    dd_op: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    children: Vec<CpuProfileRecord>,
}

/// Serialized representation of a CPU profile.
pub type CpuProfile = Vec<CpuProfileRecord>;

/// Serialized representation of an arrangement size profile record.
#[derive(Serialize)]
pub struct SizeProfileRecord {
    opid: usize,
    size: isize,
    short_descr: String,
    locations: Vec<SourcePosition>,
    descr: String,
    dd_op: String,
}

/// Serialized representation of a size profile.
#[derive(Serialize)]
pub struct SizeProfile {
    short_descr: String,
    pub children: Vec<SizeProfileRecord>,
}

impl SizeProfile {
    pub fn new(children: Vec<SizeProfileRecord>) -> Self {
        Self {
            short_descr: "Dataflow".to_string(),
            children,
        }
    }
}

/// Serialized representation of either CPU or memory profile.
#[derive(Serialize)]
#[serde(tag = "type")]
pub enum ProfileDump {
    Cpu {
        name: String,
        records: CpuProfile,
    },
    Size {
        name: String,
        records: Vec<SizeProfile>,
    },
}

// Indexed representation of `source_code` that
// supports fast lookup by `SourcePosition`.
type IndexedSourceCode = HashMap<&'static str, Vec<&'static str>>;

#[derive(Debug)]
pub struct Profile {
    source_code: &'static DDlogSourceCode,
    indexed_source_code: IndexedSourceCode,
    profile_directory: PathBuf,
    // Instant when the profiling session was started.
    start_time: Instant,
    addresses: SequenceTrie<usize, usize>,
    op_address: FnvHashMap<usize, Vec<usize>>,
    /// Map operator id to its detailed debug info.
    debug_info: FnvHashMap<usize, OperatorDebugInfo>,
    /// Short name of the op only.
    short_names: FnvHashMap<usize, String>,
    sizes: FnvHashMap<usize, isize>,
    peak_sizes: FnvHashMap<usize, isize>,
    changes: FnvHashMap<usize, isize>,
    starts: FnvHashMap<(usize, usize), Duration>,
    durations: FnvHashMap<usize, (Duration, usize)>,
    // Initialization creates a file
    timely_stats: Option<Statistics>,
    // Keep track of whether we already tried initializing timely_stats, this avoids us
    // repeatedly trying to initialize it on every event batch. If we failed once we give
    // up.
    stats_init: bool,
}

impl Profile {
    pub fn new(source_code: &'static DDlogSourceCode, profile_directory: PathBuf) -> Profile {
        let indexed_source_code: HashMap<&'static str, Vec<&'static str>> = source_code
            .code
            .iter()
            .map(|source_file| (source_file.filename, source_file.contents.lines().collect()))
            .collect();
        Profile {
            source_code,
            indexed_source_code,
            profile_directory,
            start_time: Instant::now(),
            addresses: SequenceTrie::new(),
            op_address: FnvHashMap::default(),
            debug_info: FnvHashMap::default(),
            short_names: FnvHashMap::default(),
            sizes: FnvHashMap::default(),
            peak_sizes: FnvHashMap::default(),
            changes: FnvHashMap::default(),
            starts: FnvHashMap::default(),
            durations: FnvHashMap::default(),
            timely_stats: None,
            stats_init: false,
        }
    }

    /// Extract a short code snippet of length up to `len` formatted as a single line.
    /// Replaces sequences of spaces and newlines with a single space.
    pub fn snippet(&self, source_pos: &SourcePosition, len: u32) -> String {
        match source_pos {
            SourcePosition::Range {
                file,
                start_line,
                start_col,
                end_line,
                end_col,
            } => {
                if let Some(lines) = self.indexed_source_code.get(file.as_ref()) {
                    LinesIterator::new(
                        lines,
                        *start_line,
                        *start_col,
                        Some((*end_line, *end_col)),
                        len,
                    )
                    .collect()
                } else {
                    "???".to_string()
                }
            }
            SourcePosition::Location { file, line, col } => {
                if let Some(lines) = self.indexed_source_code.get(file.as_ref()) {
                    LinesIterator::new(lines, *line, *col, None, len).collect()
                } else {
                    "???".to_string()
                }
            }
            SourcePosition::Unknown => "???".to_string(),
        }
    }

    /// Returns arrangement size profile.
    pub fn arrangement_size_profile(&self) -> Vec<SizeProfileRecord> {
        self.size_profile(&self.sizes)
    }

    /// Returns peak arrangement size profile.
    pub fn peak_arrangement_size_profile(&self) -> Vec<SizeProfileRecord> {
        self.size_profile(&self.peak_sizes)
    }

    /// Returns arrangement change profile if it's not empty.
    pub fn change_profile(&self) -> Option<Vec<SizeProfileRecord>> {
        let changes = self.size_profile(&self.changes);
        if changes.is_empty() {
            return None;
        }

        Some(changes)
    }

    pub fn cpu_profile(&self) -> Option<CpuProfile> {
        let cpu_profile = self.cpu_profile_inner(&self.addresses);
        if cpu_profile.is_empty() {
            return None;
        }

        Some(cpu_profile)
    }

    fn size_profile(&self, sizes: &FnvHashMap<usize, isize>) -> Vec<SizeProfileRecord> {
        let mut size_vec: Vec<(usize, isize)> = sizes.clone().into_iter().collect();
        size_vec.sort_by(|(_, sz1), (_, sz2)| sz1.cmp(sz2).reverse());
        let children = size_vec
            .iter()
            .filter_map(|(opid, size)| {
                let dd_op = self.short_names.get(opid).cloned().unwrap_or_default();
                // Skip empty arrangements.
                if *size == 0 {
                    return None;
                }
                match self.debug_info.get(opid) {
                    Some(debug_info) => Some(SizeProfileRecord {
                        opid: *opid,
                        size: *size,
                        short_descr: debug_info.short_descr(self),
                        locations: debug_info.source_pos().to_vec(),
                        descr: debug_info.description(),
                        dd_op,
                    }),
                    None => Some(SizeProfileRecord {
                        opid: *opid,
                        size: *size,
                        short_descr: "???".to_string(),
                        locations: vec![],
                        descr: "???".to_string(),
                        dd_op,
                    }),
                }
            })
            .collect();
        children
    }

    fn cpu_profile_inner(&self, addrs: &SequenceTrie<usize, usize>) -> CpuProfile {
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

        children
            .iter()
            .filter_map(|child| {
                let opid = child.value().unwrap();
                let (duration, invocations) = self.durations.get(opid).cloned().unwrap_or_default();
                let children = self.cpu_profile_inner(child);
                let dd_op = self.short_names.get(opid).cloned().unwrap_or_default();
                // Skip operators that were never fired.
                if invocations == 0 && children.is_empty() {
                    return None;
                }
                let obj = match self.debug_info.get(opid) {
                    Some(debug_info) => CpuProfileRecord {
                        opid: *opid,
                        cpu_us: duration.as_micros() as u64,
                        invocations,
                        short_descr: debug_info.short_descr(self),
                        locations: debug_info
                            .source_pos()
                            .iter()
                            .filter(|p| !p.is_unknown())
                            .cloned()
                            .collect(),
                        descr: debug_info.description(),
                        dd_op,
                        children,
                    },
                    None => CpuProfileRecord {
                        opid: *opid,
                        cpu_us: duration.as_micros() as u64,
                        invocations,
                        short_descr: "???".to_string(),
                        locations: vec![],
                        descr: "???".to_string(),
                        dd_op,
                        children,
                    },
                };
                Some(obj)
            })
            .collect()
    }

    pub fn update(&mut self, msg: &ProfMsg) {
        match msg {
            ProfMsg::TimelyMessage(events, profile_cpu, profile_timely) => {
                // Init stats struct for timely events. The profile_timely bool can become true
                // at any time, so we check it on every message batch arrival.
                if !self.stats_init && *profile_timely {
                    self.stats_init = true;

                    match Statistics::new("stats.csv") {
                        Ok(init_stats) => {
                            self.timely_stats = Some(init_stats);
                        }
                        Err(e) => {
                            eprintln!("Warning: Unable to create stats.csv for program profiling.");
                            eprintln!("Reason {}", e);
                            // stats stays None.
                        }
                    }
                }
                for ((duration, id, event), context) in events.iter() {
                    match event {
                        TimelyEvent::Operates(o) => {
                            let context = context.as_ref().expect(
                                "Operates events should always have valid context attached",
                            );
                            self.handle_operates(o, context);
                        }
                        event => {
                            if *profile_timely {
                                // In the None case it is totally fine to do nothing. This just means that
                                // profiling timely was on but we were unable to initialize the file.
                                if let Some(stats) = self.timely_stats.as_mut() {
                                    stats.handle_event(
                                        *duration,
                                        *id,
                                        event,
                                        &self.op_address,
                                        &self.short_names,
                                    );
                                }
                            }
                            if *profile_cpu {
                                self.handle_cpu_profiling(duration, *id, event);
                            }
                        }
                    }
                }
            }

            ProfMsg::DifferentialMessage(msg, profile_change) => {
                self.handle_differential(msg, *profile_change)
            }
        }
    }

    /// Dump profile to HTML file.
    pub fn dump(&self, label: Option<&str>) -> Result<String, String> {
        self.create_profile_directory()?;
        let profile_fname = self.profile_file(label);
        let mut all_profiles = vec![];
        if let Some(cpu_profile) = self.cpu_profile() {
            all_profiles.push(ProfileDump::Cpu {
                name: "CPU profile".to_string(),
                records: cpu_profile,
            });
        }
        all_profiles.push(ProfileDump::Size {
            name: "Arrangement size profile".to_string(),
            records: vec![SizeProfile::new(self.arrangement_size_profile())],
        });
        all_profiles.push(ProfileDump::Size {
            name: "Arrangement peak sizes".to_string(),
            records: vec![SizeProfile::new(self.peak_arrangement_size_profile())],
        });
        if let Some(change_profile) = self.change_profile() {
            all_profiles.push(ProfileDump::Size {
                name: "Counts of changes (insertions+deletions) to arrangements".to_string(),
                records: vec![SizeProfile::new(change_profile)],
            });
        }
        let all_profiles_str = serde_json::to_string(&all_profiles)
            .map_err(|e| format!("error converting profile to JSON: {}", e))?;
        let profile_js = format!("<script> var profiles = {}", all_profiles_str);
        let profile_html = PROFILER_UI_HTML.replace("<script src=\"profile.js\">", &profile_js);
        fs::write(&profile_fname, profile_html).map_err(|e| e.to_string())?;
        // Convert `profile_fname` to absolute path, but if that fails due to some
        // OS-specific quirk, return the relative path.
        let abs_profile_fname = profile_fname
            .canonicalize()
            .unwrap_or_else(|_| profile_fname.clone());
        Ok(abs_profile_fname.display().to_string())
    }

    /// Add events into relevant maps. This must always be done as we might need this information
    /// later if CPU profile or timely profile is turned on. If we don't always record it, it might
    /// be too late later.
    fn handle_operates(
        &mut self,
        OperatesEvent { id, addr, name }: &OperatesEvent,
        context: &OperatorDebugInfo,
    ) {
        self.addresses.insert(addr, *id);
        self.op_address.insert(*id, addr.clone());

        self.short_names.insert(
            *id,
            name.clone()
                .split([' ', ':'].as_ref())
                .next()
                .unwrap()
                .to_string(),
        );

        self.debug_info.insert(*id, context.clone());
    }

    // We always want to handle TimelyEvent::Operates as they are used for more than just
    // CPU profiling. Other events are only handled when profile_cpu is true.
    fn handle_cpu_profiling(&mut self, ts: &Duration, worker_id: usize, event: &TimelyEvent) {
        if let TimelyEvent::Schedule(ScheduleEvent { id, start_stop }) = event {
            match start_stop {
                StartStop::Start => {
                    self.starts.insert((*id, worker_id), *ts);
                }
                StartStop::Stop => {
                    let (total, ncalls) = self
                        .durations
                        .entry(*id)
                        .or_insert((Duration::new(0, 0), 0));
                    let start = self
                        .starts
                        .get(&(*id, worker_id))
                        .cloned()
                        .unwrap_or_else(|| {
                            eprintln!(
                                "TimelyEvent::Stop without a start for operator {}, worker {}",
                                *id, worker_id
                            );
                            Duration::new(0, 0)
                        });
                    *total += *ts - start;
                    *ncalls += 1;
                }
            }
        }
    }

    fn handle_differential(
        &mut self,
        msg: &[(Duration, usize, DifferentialEvent)],
        profile_change: bool,
    ) {
        //eprintln!("profiling message: {:?}", msg);
        for (_, _, event) in msg.iter() {
            match event {
                DifferentialEvent::Batch(x) => {
                    let size = self.sizes.entry(x.operator).or_insert(0);
                    *size += x.length as isize;
                    let peak = self.peak_sizes.entry(x.operator).or_insert(0);
                    *peak = max(*peak, *size);
                    if profile_change {
                        let changes = self.changes.entry(x.operator).or_insert(0);
                        *changes += x.length as isize;
                    }
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

    // Directory to dump profiles to.
    fn profile_directory(&self) -> PathBuf {
        // Profile directory is a concatenation of `self.profile_directory` and
        // "ddlog_profile_<THIS_PROCESS_PID>".
        let pid = std::process::id();
        PathBuf::from_iter(&[
            &self.profile_directory,
            &PathBuf::from(format!("ddlog_profile_{}", pid)),
        ])
    }

    // Generate a file name to write profile to for a given label.
    // Uses current time in nanoseconds to generate unique and ordered
    // file names for subsequent profile snapshots.
    fn profile_file(&self, label: Option<&str>) -> PathBuf {
        let timestamp = self.start_time.elapsed().as_nanos();
        let fname = if let Some(l) = label {
            format!("profile.{}.{}.html", l, timestamp)
        } else {
            format!("profile.{}.html", timestamp)
        };
        PathBuf::from_iter(&[&self.profile_directory(), &PathBuf::from(fname)])
    }

    fn create_profile_directory(&self) -> Result<(), String> {
        let mut dirs = PROFILE_DIRECTORIES
            .lock()
            .map_err(|e| format!("failed to acquire profiling mutex: {}", e))?;

        let dir = self.profile_directory();

        if dirs.contains(&dir) {
            return Ok(());
        };

        // Create directory.
        fs::create_dir_all(dir.as_path()).map_err(|e| {
            format!(
                "failed to create profiling directory '{}': {}",
                dir.display(),
                e
            )
        })?;

        // Files needed to render profile in HTML.
        let ccs_path = PathBuf::from_iter(&[&dir, &PathBuf::from(&"ui.css")]);
        fs::write(ccs_path.as_path(), PROFILER_UI_CSS)
            .map_err(|e| format!("failed to create file '{}': {}", ccs_path.display(), e))?;
        let js_path = PathBuf::from_iter(&[&dir, &PathBuf::from(&"ui.js")]);
        fs::write(js_path.as_path(), PROFILER_UI_JS)
            .map_err(|e| format!("failed to create file '{}': {}", js_path.display(), e))?;

        // Dump source code.
        let src_dir = PathBuf::from_iter(&[&dir, &PathBuf::from("src")]);
        fs::create_dir_all(src_dir.as_path()).map_err(|e| {
            format!(
                "failed to create profiling directory '{}': {}",
                src_dir.display(),
                e
            )
        })?;
        for src_file in self.source_code.code {
            let mut file_path = PathBuf::from_iter(&[&src_dir, &PathBuf::from(&src_file.filename)]);

            // Regular text files in the local filesystem cannot be accessed
            // from a HTML page.  Store .dl code as a Javascript program that
            // contains a single string.
            file_path.set_extension("dl.js");
            if let Some(dir_path) = file_path.parent() {
                fs::create_dir_all(dir_path).map_err(|e| {
                    format!("failed to create directory '{}': {}", dir_path.display(), e)
                })?;
            }

            let js_contents = format!(
                "globalMap.set({},{});",
                JsonValue::String(src_file.filename.to_string()),
                JsonValue::String(src_file.contents.to_string())
            );

            fs::write(file_path.as_path(), js_contents)
                .map_err(|e| format!("failed to create file '{}': {}", file_path.display(), e))?;
        }

        dirs.insert(dir);
        Ok(())
    }
}
