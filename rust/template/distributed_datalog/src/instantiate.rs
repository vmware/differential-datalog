//! A module for instantiating a desired configuration for a distributed
//! computation. This functionality is meant to be executed by all nodes
//! participating in the distributed computation and will take care of
//! configuring the "local" compute node accordingly, by creating a
//! `DDlogServer` instance, a `TcpReceiver`, a `TxnMux`, and file sinks
//! and sources if desired.

use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
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

    Ok(DDlogServer::new(program, redirects))
}

/// Create a transaction multiplexer wrapping the given server.
fn create_txn_mux<P>(server: DDlogServer<P>) -> Result<TxnMux<Update<DDValue>, String>, String>
where
    P: Send + DDlog + 'static,
{
    let mut txnmux = TxnMux::new();
    txnmux
        .subscribe(Box::new(server))
        .map_err(|_| "failed to subscribe DDlogServer to TxnMux")?;

    Ok(txnmux)
}

/// Add as many `TcpSender` objects as required given the provided node
/// configuration.
fn add_tcp_senders<P>(
    server: &mut DDlogServer<P>,
    node_cfg: &NodeCfg,
    accumulators: &mut HashMap<
        BTreeSet<RelId>,
        SharedObserver<DistributingAccumulator<Update<DDValue>, DDValue, String>>,
    >,
    sinks: &mut HashMap<BTreeSet<RelId>, Vec<(SinkRealization<P::Convert>, usize)>>,
    assignment: &Assignment,
) -> Result<(), String>
where
    P: DDlog,
{
    deduce_outputs(node_cfg, assignment)?
        .into_iter()
        .try_for_each(|(addr, rel_ids)| {
            match addr {
                Addr::Ip(addr) => {
                    let sender = TcpSender::new(addr)
                        .map_err(|e| format!("failed to create TcpSender socket: {}", e))?;
                    let sender = Arc::new(Mutex::new(sender));

                    // TODO: What should we really do if we can't subscribe?
                    let accumulator = accumulators
                        .entry(rel_ids.clone())
                        .or_insert_with(|| Arc::new(Mutex::new(DistributingAccumulator::new())));

                    let subscription = accumulator
                        .lock()
                        .unwrap()
                        .subscribe(Box::new(sender.clone()))
                        .map_err(|_| "failed to subscribe TCP sender".to_string())?;

                    sinks
                        .entry(rel_ids.clone())
                        .or_insert_with(Vec::new)
                        .insert(0, (SinkRealization::Node(sender), subscription));

                    server
                        .add_stream(rel_ids)
                        .subscribe(Box::new(accumulator.clone()))
                        .map_err(|_| "failed to subscribe accumulator".to_string())
                }
            }
        })
}

/// Add a `TcpReceiver` feeding the given server if one is needed given
/// the provided node configuration.
fn add_tcp_receiver<P>(
    txnmux: &mut TxnMux<Update<DDValue>, String>,
    addr: &SocketAddr,
    sources: &mut HashMap<
        Source,
        (
            Option<SourceRealization<P::Convert>>,
            SharedObserver<DistributingAccumulator<Update<DDValue>, DDValue, String>>,
            usize,
        ),
    >,
) -> Result<(), String>
where
    P: Send + DDlog + 'static,
    P::Convert: Send,
{
    let receiver =
        TcpReceiver::new(addr).map_err(|e| format!("failed to create TcpReceiver: {}", e))?;
    let receiver = Arc::new(Mutex::new(receiver));
    let accumulator = Arc::new(Mutex::new(DistributingAccumulator::new()));

    match txnmux.add_observable(Box::new(receiver.clone())) {
        Ok(id) => {
            let _ = sources.insert(
                Source::TcpReceiver,
                (Some(SourceRealization::Node(receiver)), accumulator, id),
            );
            Ok(())
        }
        Err(_) => Err("failed to register TcpReceiver with TxnMux".to_string()),
    }
}

/// Deduce a mapping from file sink to a list of relation IDs for the
/// given node configuration.
fn deduce_sinks_or_sources(node_cfg: &NodeCfg, sinks: bool) -> BTreeMap<&Path, BTreeSet<RelId>> {
    node_cfg
        .iter()
        .fold(BTreeMap::new(), |map, (relid, rel_cfgs)| {
            rel_cfgs.iter().fold(map, |mut map, rel_cfg| {
                match rel_cfg {
                    RelCfg::Sink(sink) if sinks => match sink {
                        Sink::File(path) => {
                            let _ = map.entry(path).or_default().insert(*relid);
                        }
                    },
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

/// Add file sinks to the given server object, as per the node
/// configuration.
fn add_file_sinks<P>(
    server: &mut DDlogServer<P>,
    node_cfg: &NodeCfg,
    accumulators: &mut HashMap<
        BTreeSet<RelId>,
        SharedObserver<DistributingAccumulator<Update<DDValue>, DDValue, String>>,
    >,
    sinks: &mut HashMap<BTreeSet<RelId>, Vec<(SinkRealization<P::Convert>, usize)>>,
) -> Result<(), String>
where
    P: Send + DDlog + 'static,
    P::Convert: Send,
{
    deduce_sinks_or_sources(node_cfg, true)
        .iter()
        .try_for_each(|(path, rel_ids)| {
            let file = File::create(path)
                .map_err(|e| format!("failed to create file {}: {}", path.display(), e))?;
            let sink = Arc::new(Mutex::new(FileSink::<P::Convert>::new(file)));

            let accumulator = accumulators
                .entry(rel_ids.clone())
                .or_insert_with(|| Arc::new(Mutex::new(DistributingAccumulator::new())));

            let subscription = accumulator
                .lock()
                .unwrap()
                .subscribe(Box::new(sink.clone()))
                .map_err(|_| {
                    format!(
                        "failed to subscribe file sink {} to accumulator",
                        path.display()
                    )
                })?;

            sinks
                .entry(rel_ids.clone())
                .or_insert_with(Vec::new)
                .insert(0, (SinkRealization::File(sink), subscription));

            server
                .add_stream(rel_ids.clone())
                .subscribe(Box::new(accumulator.clone()))
                .map_err(|_| "failed to subscribe accumulator to DDlogServer".to_string())
        })
}

/// Realize the given configuration locally.
// TODO: Right now this function assumes a pristine state (i.e., nothing
//       had been created previously), however we really would want to
//       transition from a previously created state (which happens to be
//       "empty" initially) to the given one.
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

    let mut server = create_server::<P>(&node_cfg)?;
    let mut sources = HashMap::new();
    let mut accumulators = HashMap::new();
    let mut sinks = HashMap::new();

    add_tcp_senders(
        &mut server,
        node_cfg,
        &mut accumulators,
        &mut sinks,
        assignment,
    )?;
    add_file_sinks(&mut server, node_cfg, &mut accumulators, &mut sinks)?;

    let mut txnmux = create_txn_mux(server)?;
    match addr {
        Addr::Ip(addr) => add_tcp_receiver::<P>(&mut txnmux, addr, &mut sources)?,
    }

    let mut realization = Realization {
        _sources: sources,
        _txnmux: txnmux,
        _accumulators: accumulators,
        _sinks: sinks,
    };

    realization.add_file_sources(node_cfg)?;

    println!(
        "realized node configuration locally in {} ms",
        now.elapsed().as_millis()
    );

    Ok(realization)
}

/// All possible sources of a Realization
#[derive(Debug)]
enum SourceRealization<C>
where
    C: DDlogConvert,
{
    File(Arc<Mutex<FileSource<C>>>),
    Node(Arc<Mutex<TcpReceiver<Update<DDValue>>>>),
}

/// All possible sinks of a Realization
#[derive(Debug)]
enum SinkRealization<C>
where
    C: DDlogConvert,
{
    File(SharedObserver<FileSink<C>>),
    Node(SharedObserver<TcpSender<Update<DDValue>>>),
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
            Option<SourceRealization<P::Convert>>,
            SharedObserver<DistributingAccumulator<Update<DDValue>, DDValue, String>>,
            usize,
        ),
    >,
    /// The transaction multiplexer as input to the DDLogServer
    _txnmux: TxnMux<Update<DDValue>, String>,
    /// All sink accumulators of this realization to connect new nodes to
    _accumulators: HashMap<
        BTreeSet<RelId>,
        SharedObserver<DistributingAccumulator<Update<DDValue>, DDValue, String>>,
    >,
    /// All sinks of this realization with their subscription
    _sinks: HashMap<BTreeSet<RelId>, Vec<(SinkRealization<P::Convert>, usize)>>,
}

impl<P> Realization<P>
where
    P: Send + DDlog + 'static,
    P::Convert: Send + DDlogConvert,
{
    /// Remove a source from the existing realization.
    /// Also clear the accumulator and disconnect
    /// from the TxnMux.
    pub fn remove_source(&mut self, src: &Source) {
        // Remove entry.
        let (_, accumulator, id) = self._sources.remove(src).unwrap();
        let mut accum_observ = accumulator.lock().unwrap();

        // Clear accumulator.
        accum_observ.clear();

        // Disconnect from TxnMux.
        self._txnmux.remove_observable(id);
    }

    /// Add a source to an existing realization.
    pub fn add_source(&mut self, path: &Path) -> Result<(), String> {
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

    /// Add file sources as per the node configuration to the TxnMux for this
    /// Realization.
    fn add_file_sources(&mut self, node_cfg: &NodeCfg) -> Result<(), String> {
        deduce_sinks_or_sources(node_cfg, false)
            .iter()
            .try_for_each(|(path, _rel_ids)| self.add_source(path))
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
