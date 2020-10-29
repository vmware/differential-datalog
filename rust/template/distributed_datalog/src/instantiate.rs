//! A module for instantiating a desired configuration for a distributed
//! computation. This functionality is meant to be executed by all nodes
//! participating in the distributed computation and will take care of
//! configuring the "local" compute node accordingly, by creating a
//! `DDlogServer` instance, a `TcpReceiver`, a `TxnMux`, and file sinks
//! and sources if desired.

use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::fs::File;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use differential_datalog::ddval::DDValue;
use differential_datalog::program::RelId;
use differential_datalog::program::Update;
use differential_datalog::record::Record;
use differential_datalog::{DDlog, DDlogConvert};

use crate::accumulate::Accumulator;
use crate::accumulate::DistributingAccumulator;
use crate::observe::Observable;
use crate::observe::SharedObserver;
use crate::observe::UpdatesObservable;
use crate::schema::Addr;
use crate::schema::Node;
use crate::schema::NodeCfg;
use crate::schema::RelCfg;
use crate::schema::Sink;
use crate::schema::Source;
use crate::schema::SysCfg;
use crate::sinks::File as FileSink;
use crate::sources::File as FileSource;
use crate::tcp_channel::TcpReceiver;
use crate::tcp_channel::TcpSender;
use crate::txnmux::TxnMux;
use crate::DDlogServer;

use serde::de::DeserializeOwned;
use serde::Serialize;

/// A mapping from member address to relation IDs used for describing
/// output relationships.
pub type Outputs = BTreeMap<Addr, BTreeSet<RelId>>;
/// A mapping from abstract nodes to actual members in the system.
pub type Assignment = BTreeMap<Node, Addr>;

/// Deduce the output streaming relations we require.
fn deduce_outputs(node_cfg: &NodeCfg, assignment: &Assignment) -> Result<Outputs, String> {
    node_cfg
        .iter()
        .try_fold(Outputs::new(), |outputs, (rel_id, rel_cfgs)| {
            rel_cfgs.iter().try_fold(outputs, |mut outputs, rel_cfg| {
                match rel_cfg {
                    RelCfg::Output(node, _dst) => {
                        let addr = assignment.get(node).ok_or_else(|| {
                            format!(
                                "failed to find node {} in assignment",
                                node.to_hyphenated_ref()
                            )
                        })?;
                        let rels = outputs.entry(*addr).or_default();
                        let _ = rels.insert(*rel_id);
                    }
                    RelCfg::Input(..) | RelCfg::Source(..) | RelCfg::Sink(..) => (),
                }
                Ok(outputs)
            })
        })
}

/// Deduce the required redirections for a given node configuration.
///
/// Redirections determine what input relations the inputs to a certain
/// node feed into.
fn deduce_redirects(config: &NodeCfg) -> HashMap<RelId, RelId> {
    config.iter().fold(HashMap::new(), |redirects, (rel, cfg)| {
        cfg.iter().fold(redirects, |mut redirects, rel_cfg| {
            match rel_cfg {
                RelCfg::Input(src) => {
                    // Each of the inputs needs to be redirected to the
                    // relation it feeds into.
                    let _ = redirects.insert(*src, *rel);
                }
                RelCfg::Output(..) | RelCfg::Source(..) | RelCfg::Sink(..) => (),
            }
            redirects
        })
    })
}

/// Create a `DDlogServer` as per the given node configuration.
fn create_server<P>(node_cfg: &NodeCfg) -> Result<DDlogServer<P>, String>
where
    P: Send + DDlog,
{
    let redirects = deduce_redirects(node_cfg);
    // TODO: Should the number of workers be made configurable?
    let (program, _) = P::run(2, false, |_, _: &Record, _| {})?;

    Ok(DDlogServer::new(Some(program), redirects))
}

/// Deduce a mapping from file sink to a list of relation IDs for the
/// given node configuration.
fn deduce_sinks_or_sources(node_cfg: &NodeCfg, sinks: bool) -> BTreeMap<&Path, BTreeSet<RelId>> {
    node_cfg
        .iter()
        .fold(BTreeMap::new(), |map, (relid, rel_cfgs)| {
            rel_cfgs.iter().fold(map, |mut map, rel_cfg| {
                match rel_cfg {
                    RelCfg::Sink(sink) if sinks => {
                        if let Sink::File(path) = sink {
                            let _ = map.entry(path).or_default().insert(*relid);
                        }
                    }
                    RelCfg::Source(source) if !sinks => {
                        if let Source::File(path) = source {
                            let _ = map.entry(path).or_default().insert(*relid);
                        }
                    }
                    _ => (),
                };
                map
            })
        })
}

/// Realize the given configuration locally.
fn realize<P>(
    addr: &Addr,
    node_cfg: &NodeCfg,
    assignment: &Assignment,
) -> Result<Realization<P>, String>
where
    P: Send + DDlog + 'static,
    P::Convert: Send + DDlogConvert,
{
    let now = Instant::now();
    let mut realization = Realization::new();
    let mut server = create_server::<P>(&node_cfg)?;

    realization.add_tcp_senders(node_cfg, &mut server, assignment)?;
    realization.add_file_sinks(node_cfg, &mut server)?;
    realization.subscribe_txnmux(server)?;
    match addr {
        Addr::Ip(addr) => realization.add_tcp_receiver(addr)?,
    }
    realization.add_file_sources(node_cfg)?;

    println!(
        "realized node configuration locally in {} ms",
        now.elapsed().as_millis()
    );

    Ok(realization)
}

/// All possible sources of a Realization
#[derive(Debug)]
enum SourceRealization<C, D>
where
    C: DDlogConvert,
    D: DeserializeOwned + Debug + Into<Update<DDValue>> + Send,
{
    File(Arc<Mutex<FileSource<C>>>),
    Node(Arc<Mutex<TcpReceiver<Update<DDValue>, D>>>),
}

/// All possible sinks of a Realization
#[derive(Debug)]
enum SinkRealization<C, S>
where
    C: DDlogConvert,
    S: Serialize + Debug + Send + 'static,
{
    File(SharedObserver<FileSink<C>>),
    Node(SharedObserver<TcpSender<S>>),
}

/// An object representing a realized configuration.
///
/// Right now all that clients can do with an object of this type is
/// dropping it to tear everything down.
#[derive(Debug)]
pub struct Realization<P>
where
    P: Send + DDlog + 'static,
    P::Convert: Send + DDlogConvert,
{
    /// All sources of this realization and the subscription the node has to them
    _sources: HashMap<
        Source,
        (
            Option<SourceRealization<P::Convert, P::UpdateSerializer>>,
            SharedObserver<DistributingAccumulator<Update<DDValue>, DDValue, String>>,
            usize,
        ),
    >,
    /// The transaction multiplexer as input to the DDLogServer
    _txnmux: TxnMux<Update<DDValue>, String>,
    /// All sinks of this realization with their subscription
    _sinks: HashMap<
        BTreeSet<RelId>,
        (
            SharedObserver<DistributingAccumulator<Update<DDValue>, DDValue, String>>,
            UpdatesObservable<Update<DDValue>, String>,
            HashMap<Sink, (SinkRealization<P::Convert, P::UpdateSerializer>, usize)>,
        ),
    >,
}

impl<P> Default for Realization<P>
where
    P: Send + DDlog + 'static,
    P::Convert: Send + DDlogConvert,
{
    fn default() -> Self {
        Realization {
            _sinks: HashMap::new(),
            _sources: HashMap::new(),
            _txnmux: TxnMux::new(),
        }
    }
}

impl<P> Realization<P>
where
    P: Send + DDlog + 'static,
    P::Convert: Send + DDlogConvert,
{
    /// Instantiates a new, default Realization.
    pub fn new() -> Realization<P> {
        Default::default()
    }

    /// Subscribe the `TxnMux` of the existing realization to the given server.
    pub fn subscribe_txnmux(&mut self, server: DDlogServer<P>) -> Result<(), &str> {
        self._txnmux
            .subscribe(Box::new(server))
            .map_err(|_| "failed to subscribe DDlogServer to TxnMux")
    }

    /// Remove a source from the existing realization.
    /// Also clear the accumulator and disconnect
    /// from the TxnMux.
    pub fn remove_source(&mut self, src: &Source) -> Result<(), &str> {
        // Remove entry.
        let (_, accumulator, id) = self._sources.remove(src).unwrap();
        let mut accum_observ = accumulator.lock().unwrap();

        // Clear accumulator.
        if accum_observ.clear().is_ok() {
            // Disconnect from TxnMux.
            self._txnmux.remove_observable(id);
            Ok(())
        } else {
            Err("Cannot remove source, transaction in progress")
        }
    }

    /// Add a `TcpReceiver` to the existing realization feeding the given server
    /// if one is needed given the provided node configuration.
    pub fn add_tcp_receiver(&mut self, addr: &SocketAddr) -> Result<(), String> {
        let receiver =
            TcpReceiver::new(addr).map_err(|e| format!("failed to create TcpReceiver: {}", e))?;
        let receiver = Arc::new(Mutex::new(receiver));
        // FIXME: Eventually need to find a way to connect the accumulator to
        // the receiver.
        let accumulator = Arc::new(Mutex::new(DistributingAccumulator::new()));

        match self._txnmux.add_observable(Box::new(receiver.clone())) {
            Ok(id) => {
                let _ = self._sources.insert(
                    Source::TcpReceiver,
                    (Some(SourceRealization::Node(receiver)), accumulator, id),
                );
                Ok(())
            }
            Err(_) => Err("failed to register TcpReceiver with TxnMux".to_string()),
        }
    }

    /// Add a file source to an existing realization.
    pub fn add_file_source(&mut self, path: &Path) -> Result<(), String> {
        let mut source = Arc::new(Mutex::new(FileSource::<P::Convert>::new(path)));

        let accumulator = Arc::new(Mutex::new(DistributingAccumulator::new()));

        match self._txnmux.add_observable(Box::new(accumulator.clone())) {
            Ok(id) => {
                source
                    .subscribe(Box::new(accumulator.clone()))
                    .map_err(|_| {
                        format!(
                            "failed to add file source {} to accumulator",
                            path.display()
                        )
                    })?;

                let pathbuf = PathBuf::from(path);
                let _ = self._sources.insert(
                    Source::File(pathbuf),
                    (Some(SourceRealization::File(source)), accumulator, id),
                );
                Ok(())
            }
            Err(_) => Err("failed to register Accumulator with TxnMux".to_string()),
        }
    }

    /// Remove file sink or tcp sender from an existing Realization.
    /// Locate the entry in _sinks whose map contains the sink.
    /// Remove the entry from the sink map.
    /// Unsubscribe the accumulator from the sink.
    /// Return an error if unable to find a sink map containing the sink.
    pub fn remove_sink(&mut self, sink: &Sink) -> Result<(), String> {
        for (accumulator, _, sink_map) in self._sinks.values_mut() {
            if sink_map.contains_key(sink) {
                let (_, subscription) = sink_map.remove(sink).unwrap();
                let _ = accumulator.unsubscribe(&subscription);
                return Ok(());
            }
        }
        let err = format!(
            "Failed to remove sink: {:?}, not found in realization",
            sink
        );
        Err(err)
    }

    /// Add file sink or tcp sender to an existing Realization.
    /// Creates and adds accumulator for this rel_ids to the Realization if needed.
    /// Subscribes the accumulator to the sink and adds the sink to
    /// the accumulator's map.
    pub fn add_sink(
        &mut self,
        sink: &Sink,
        rel_ids: BTreeSet<RelId>,
        server: &mut DDlogServer<P>,
    ) -> Result<(), String> {
        // Add the accumulator to the realization if needed.
        self.add_sink_accumulator(rel_ids.clone(), server)?;
        let (accumulator, _, sink_map) = self._sinks.get_mut(&rel_ids).unwrap();

        match sink {
            Sink::File(path) => {
                let file = File::create(path)
                    .map_err(|e| format!("failed to create file {}:, {}", path.display(), e))?;
                let file_sink = Arc::new(Mutex::new(FileSink::<P::Convert>::new(file)));

                // Subscribe the accumulator to this sink.
                let subscription = accumulator
                    .lock()
                    .unwrap()
                    .subscribe(Box::new(file_sink.clone()))
                    .map_err(|_| "failed to subscribe file sink to accumulator".to_string())?;

                // Add sink to sink map for this accumulator.
                let _ = sink_map.insert(
                    sink.clone(),
                    (SinkRealization::File(file_sink), subscription),
                );
            }
            Sink::TcpSender(addr) => {
                match addr {
                    Addr::Ip(address) => {
                        let tcp_sender = TcpSender::new(*address)
                            .map_err(|e| format!("failed to create TcpSender socket: {}", e))?;
                        let sink = Arc::new(Mutex::new(tcp_sender));

                        // Subscribe the accumulator to this sink.
                        let subscription = accumulator
                            .lock()
                            .unwrap()
                            .subscribe(Box::new(sink.clone()))
                            .map_err(|_| "failed to subscribe TcpSender sink to accumulator")?;

                        // Add sink to sink map for this accumulator.
                        let _ = sink_map.insert(
                            Sink::TcpSender(*addr),
                            (SinkRealization::Node(sink), subscription),
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Remove the sink accumulator from the realization.
    /// Remove accumulator's entry (keyed by rel_ids) from _sinks.
    /// Disconnect accumulator from the server:
    /// - Unsubscribe the stream from the accumulator.
    /// - Remove the stream from the server.
    /// Clear the accumulator.
    pub fn remove_sink_accumulator(
        &mut self,
        rel_ids: BTreeSet<RelId>,
        server: &mut DDlogServer<P>,
    ) -> Result<(), String> {
        if let Some(entry) = self._sinks.remove(&rel_ids) {
            let (_, mut stream, sink_map) = entry;
            if sink_map.is_empty() {
                // Disconnect accumulator from server.
                // First, unsubscribe the stream from the accumulator.
                let _ = stream.unsubscribe(&());
                // Next, remove the stream from the server.
                server.remove_stream(stream);
                Ok(())
            } else {
                Err("Cannot remove accumulator, still subscribed to sinks".to_string())
            }
        } else {
            Err("Unable to locate accumulator to remove from _sinks".to_string())
        }
    }

    /// Creates and adds a sink accumulator to the existing Realization.
    /// Adds a stream to the server for the accumulator.
    /// Creates an entry in Realization _sinks for this rel_ids.
    pub fn add_sink_accumulator(
        &mut self,
        rel_ids: BTreeSet<RelId>,
        server: &mut DDlogServer<P>,
    ) -> Result<(), String> {
        if !self._sinks.contains_key(&rel_ids) {
            let accumulator = Arc::new(Mutex::new(DistributingAccumulator::new()));
            let mut update_observ = server.add_stream(rel_ids.clone());
            update_observ
                .subscribe(Box::new(accumulator.clone()))
                .map_err(|_| "failed to subscribe accumulator to DDlogServer".to_string())?;

            let _ = self
                ._sinks
                .insert(rel_ids, (accumulator, update_observ, HashMap::new()));
        }
        Ok(())
    }

    /// Checks that the given file source exists in the Realization.
    /// Used for testing.
    pub fn contains_file_source(&self, path: PathBuf) -> bool {
        self._sources.contains_key(&Source::File(path))
    }

    /// Checks that a tcp receiver exists in the Realization.
    /// Used for testing.
    pub fn contains_tcp_receiver(&self) -> bool {
        self._sources.contains_key(&Source::TcpReceiver)
    }

    /// Retrieves the id for the source (generated by the TxnMux).
    /// Used for testing.
    pub fn get_source_id(&self, path: PathBuf) -> usize {
        let (_, _, id) = self._sources.get(&Source::File(path)).unwrap();
        *id
    }

    /// Retrieves the id for the TcpReceiver in the Realization.
    /// Used for testing.
    pub fn get_tcp_receiver_id(&self) -> usize {
        let (_, _, id) = self._sources.get(&Source::TcpReceiver).unwrap();
        *id
    }

    /// Checks that the given source (checking by its id) is contained
    /// in the TxnMux's subscriptions.
    /// Used for testing.
    pub fn txn_subscription_exists(&self, id: usize) -> bool {
        self._txnmux.subscription_exists(id)
    }

    /// Checks that the given sink both has an associated accumulator and
    /// that said accumulator is subscribed to the sink.
    /// Used for testing.
    pub fn accumulator_is_subscribed_to_sink(&self, rel_ids: BTreeSet<RelId>, sink: &Sink) -> bool {
        let (_, _, sink_map) = self._sinks.get(&rel_ids).unwrap();
        sink_map.contains_key(sink)
    }

    /// Add file sources as per the node configuration to the TxnMux for this
    /// Realization.
    fn add_file_sources(&mut self, node_cfg: &NodeCfg) -> Result<(), String> {
        deduce_sinks_or_sources(node_cfg, false)
            .iter()
            .try_for_each(|(path, _rel_ids)| self.add_file_source(path))
    }

    /// Add file sinks to the given server object, as per the node
    /// configuration.
    fn add_file_sinks(
        &mut self,
        node_cfg: &NodeCfg,
        server: &mut DDlogServer<P>,
    ) -> Result<(), String> {
        deduce_sinks_or_sources(node_cfg, true)
            .iter()
            .try_for_each(|(path, rel_ids)| {
                let file = PathBuf::from(path);
                let file_sink = Sink::File(file);
                self.add_sink(&file_sink, rel_ids.clone(), server)
            })
    }

    /// Add as many `TcpSender` objects as required given the provided node
    /// configuration.
    fn add_tcp_senders(
        &mut self,
        node_cfg: &NodeCfg,
        server: &mut DDlogServer<P>,
        assignment: &Assignment,
    ) -> Result<(), String> {
        deduce_outputs(node_cfg, assignment)?
            .into_iter()
            .try_for_each(|(addr, rel_ids)| {
                let addr_sink = Sink::TcpSender(addr);
                self.add_sink(&addr_sink, rel_ids, server)
            })
    }
}

/// Instantiate a configuration on a particular node under the given
/// assignment.
pub fn instantiate<P>(
    sys_cfg: SysCfg,
    addr: &Addr,
    assignment: &Assignment,
) -> Result<Vec<Realization<P>>, String>
where
    P: Send + DDlog + 'static,
    P::Convert: Send + DDlogConvert,
{
    assignment
        .iter()
        .filter_map(|(uuid, assigned_addr)| {
            if assigned_addr == addr {
                sys_cfg.get(uuid)
            } else {
                None
            }
        })
        .try_fold(Vec::new(), |mut accumulator, node_cfg| {
            realize::<P>(addr, node_cfg, assignment).map(|realization| {
                accumulator.push(realization);
                accumulator
            })
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use maplit::btreemap;
    use maplit::btreeset;

    use uuid::Uuid;

    use crate::schema::Source;

    #[test]
    fn file_sink_deduction() {
        let node_cfg = btreemap! {
            0 => btreeset! {
                RelCfg::Source(Source::File(PathBuf::from("input.dat"))),
                RelCfg::Sink(Sink::File(PathBuf::from("output_0_2.dump"))),
            },
            1 => btreeset! {
                RelCfg::Input(2),
                RelCfg::Input(4),
            },
            2 => btreeset! {
                RelCfg::Sink(Sink::File(PathBuf::from("output_0_2.dump"))),
            },
            3 => btreeset! {
                RelCfg::Sink(Sink::File(PathBuf::from("output_3.dump"))),
                RelCfg::Input(0),
            },
        };

        let sinks = deduce_sinks_or_sources(&node_cfg, true);
        assert_eq!(sinks.len(), 2);

        let rel_ids = sinks.get(Path::new("output_0_2.dump")).unwrap();
        assert_eq!(rel_ids.len(), 2);
        assert!(rel_ids.contains(&0));
        assert!(rel_ids.contains(&2));

        let rel_ids = sinks.get(Path::new("output_3.dump")).unwrap();
        assert_eq!(rel_ids.len(), 1);
        assert!(rel_ids.contains(&3));
    }

    #[test]
    fn output_deduction_failure() {
        let uuid0 = Uuid::new_v4();
        let uuid1 = Uuid::new_v4();
        let node0 = Addr::Ip("127.0.0.1:1".parse().unwrap());

        let node0_cfg = btreemap! {
            0 => btreeset! {
                RelCfg::Source(Source::File(PathBuf::from("input.cmd"))),
                RelCfg::Output(uuid1, 1),
            },
        };
        let assignment = btreemap! {
            uuid0 => node0.clone(),
            // uuid1 does not have an assignment.
        };

        assert!(deduce_outputs(&node0_cfg, &assignment).is_err());
    }

    #[test]
    fn output_deduction_two_nodes() {
        let uuid0 = Uuid::new_v4();
        let uuid1 = Uuid::new_v4();
        let node0 = Addr::Ip("127.0.0.1:1".parse().unwrap());
        let node1 = Addr::Ip("127.0.0.1:2".parse().unwrap());

        let node0_cfg = btreemap! {
            0 => btreeset! {
                RelCfg::Source(Source::File(PathBuf::from("input.cmd"))),
                RelCfg::Output(uuid1, 1),
            },
        };
        let node1_cfg = btreemap! {
            1 => btreeset! {
                RelCfg::Input(0),
            },
        };
        let assignment = btreemap! {
            uuid0 => node0.clone(),
            uuid1 => node1.clone(),
        };

        let outputs = deduce_outputs(&node0_cfg, &assignment).unwrap();
        let expected = btreemap! {
            node1.clone() => btreeset! { 0 },
        };
        assert_eq!(outputs, expected);

        let outputs = deduce_outputs(&node1_cfg, &assignment).unwrap();
        assert_eq!(outputs, Outputs::new());
    }

    #[test]
    fn output_deduction_three_nodes() {
        let uuid0 = Uuid::new_v4();
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let node0 = Addr::Ip("127.0.0.1:1".parse().unwrap());
        let node1 = Addr::Ip("127.0.0.1:2".parse().unwrap());
        let node2 = Addr::Ip("127.0.0.1:3".parse().unwrap());

        let node0_cfg = btreemap! {
            0 => btreeset!{
                RelCfg::Source(Source::File("input0.dat".into())),
            },
            1 => btreeset!{
                RelCfg::Output(uuid2, 4),
            },
        };
        let node1_cfg = btreemap! {
            2 => btreeset!{
                RelCfg::Source(Source::File("input2.dat".into())),
            },
            3 => btreeset!{
                RelCfg::Output(uuid2, 6),
            }
        };
        let node2_cfg = btreemap! {
            4 => btreeset!{
                RelCfg::Input(1)
            },
            5 => btreeset!{
                RelCfg::Input(3)
            },
            6 => btreeset!{
                RelCfg::Sink(Sink::File("node2.dump".into())),
            },
        };

        let assignment = btreemap! {
            uuid0 => node0.clone(),
            uuid1 => node1.clone(),
            uuid2 => node2.clone(),
        };

        let outputs = deduce_outputs(&node0_cfg, &assignment).unwrap();
        let expected = btreemap! {
            node2.clone() => btreeset! { 1 },
        };
        assert_eq!(outputs, expected);

        let outputs = deduce_outputs(&node1_cfg, &assignment).unwrap();
        let expected = btreemap! {
            node2.clone() => btreeset! { 3 },
        };
        assert_eq!(outputs, expected);

        let outputs = deduce_outputs(&node2_cfg, &assignment).unwrap();
        let expected = btreemap! {};
        assert_eq!(outputs, expected);
    }
}
