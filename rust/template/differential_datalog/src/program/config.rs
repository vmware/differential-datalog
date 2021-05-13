//! Configuration for DDlog programs

use crate::{
    profile::Profile,
    program::{worker::ProfilingData, Program, PROF_MSG_BUF_SIZE},
};
use differential_dataflow::Config as DDFlowConfig;
use std::{
    env,
    sync::{atomic::AtomicBool, Mutex},
    thread::{self, JoinHandle},
};
use timely::Config as TimelyConfig;
use triomphe::Arc;

/// The configuration for a DDlog program
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// The number of timely
    pub num_timely_workers: usize,
    /// Whether extra regions should be added to the dataflow
    ///
    /// These extra regions *significantly* help with the readability
    /// of the generated dataflows at the cost of a minor performance
    /// penalty. Best used with [`ProfilingKind::TimelyProfiling`]
    /// in order to visualize the dataflow graph
    pub enable_debug_regions: bool,
    /// The kind of profiling to enable
    pub profiling_kind: ProfilingKind,
    /// An amount of arrangement effort to spend each scheduling quantum
    ///
    /// See [`differential_dataflow::Config`]
    pub differential_idle_merge_effort: Option<isize>,
}

impl Config {
    /// Create a new [`Config`] with the default settings
    pub fn new() -> Self {
        Self {
            num_timely_workers: 1,
            enable_debug_regions: false,
            profiling_kind: ProfilingKind::default(),
            differential_idle_merge_effort: None,
        }
    }

    pub(super) fn timely_config(&self) -> Result<TimelyConfig, String> {
        let mut config = TimelyConfig::process(self.num_timely_workers);

        // Allow configuring the merge behavior of ddflow
        let idle_merge_effort = if self.differential_idle_merge_effort.is_some() {
            self.differential_idle_merge_effort

        // Support for previous users who rely on the `DIFFERENTIAL_EAGER_MERGE` variable
        // TODO: Remove the env var and expose this in all user apis
        } else if let Ok(value) = env::var("DIFFERENTIAL_EAGER_MERGE") {
            if value.is_empty() {
                None
            } else {
                let merge_effort: isize = value.parse().map_err(|_| {
                    "the `DIFFERENTIAL_EAGER_MERGE` variable must be set to an integer value"
                        .to_owned()
                })?;

                Some(merge_effort)
            }
        } else {
            None
        };

        differential_dataflow::configure(&mut config.worker, &DDFlowConfig { idle_merge_effort });

        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// The kind of profiling to be enabled for DDlog
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfilingKind {
    /// Disable all profiling
    None,
    /// Enable self-profiling
    ///
    /// Note: This spawns an additional thread and can have a
    /// performance impact on the target program and also disables
    /// general-purpose Timely Dataflow and Differential Dataflow
    /// profiling
    SelfProfiling,
    /// Enable profiling for Timely Dataflow
    TimelyProfiling {
        /// Enable profiling for Differential Dataflow as well as Timely
        differential_dataflow: bool,
    },
}

impl ProfilingKind {
    /// Returns `true` if the profiling_kind is [`None`]
    pub const fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// Returns `true` if the profiling_kind is [`SelfProfiling`]
    pub const fn is_self_profiling(&self) -> bool {
        matches!(self, Self::SelfProfiling)
    }

    /// Returns `true` if the profiling_kind is [`TimelyProfiling`]
    pub const fn is_timely_profiling(&self) -> bool {
        matches!(self, Self::TimelyProfiling { .. })
    }
}

impl Default for ProfilingKind {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug)]
pub(super) struct SelfProfilingRig {
    pub(super) profile: Option<Arc<Mutex<Profile>>>,
    pub(super) profile_thread: Option<JoinHandle<()>>,
    pub(super) profiling_data: Option<ProfilingData>,
    pub(super) profile_cpu: Option<Arc<AtomicBool>>,
    pub(super) profile_timely: Option<Arc<AtomicBool>>,
}

impl SelfProfilingRig {
    /// Create a new self profiling rig
    ///
    /// Note: Spawns a worker thread to process profiling messages
    pub(super) fn new(config: &Config) -> Self {
        if config.profiling_kind.is_self_profiling() {
            let (profile_send, profile_recv) = crossbeam_channel::bounded(PROF_MSG_BUF_SIZE);

            // Profiling data structure
            let profile = Arc::new(Mutex::new(Profile::new()));

            let (profile_cpu, profile_timely) = (
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
            );

            // Thread to collect profiling data.
            let cloned_profile = profile.clone();
            let profile_thread =
                thread::spawn(move || Program::prof_thread_func(profile_recv, cloned_profile));

            let profiling_data =
                ProfilingData::new(profile_cpu.clone(), profile_timely.clone(), profile_send);

            Self {
                profile: Some(profile),
                profile_thread: Some(profile_thread),
                profiling_data: Some(profiling_data),
                profile_cpu: Some(profile_cpu),
                profile_timely: Some(profile_timely),
            }
        } else {
            Self {
                profile: None,
                profile_thread: None,
                profiling_data: None,
                profile_cpu: None,
                profile_timely: None,
            }
        }
    }
}
