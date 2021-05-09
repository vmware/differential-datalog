/*
Copyright (c) 2021 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/* Functions and transformers for use in graph processing */

use differential_dataflow::algorithms::graphs::propagate;
use differential_dataflow::algorithms::graphs::scc;
use differential_dataflow::collection::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::consolidate::Consolidate;
use differential_dataflow::operators::ThresholdTotal;
use std::mem;
use timely::dataflow::scopes::Scope;
use timely::order::TotalOrder;

pub fn SCC<S, V, E, N, EF, LF>(
    edges: &Collection<S, V, Weight>,
    _edges: EF,
    from: fn(&E) -> N,
    to: fn(&E) -> N,
    _scclabels: LF,
) -> (Collection<S, V, Weight>)
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    V: differential_dataflow::Data,
    N: differential_dataflow::ExchangeData + std::hash::Hash,
    E: differential_dataflow::ExchangeData,
    EF: Fn(V) -> E + 'static,
    LF: Fn(ddlog_std::tuple2<N, N>) -> V + 'static,
{
    let pairs = edges.map(move |v| {
        let e = _edges(v);
        (from(&e), to(&e))
    });

    /* Recursively trim nodes without incoming and outgoing edges */
    let trimmed = scc::trim(&scc::trim(&pairs).map_in_place(|x| mem::swap(&mut x.0, &mut x.1)))
        .map_in_place(|x| mem::swap(&mut x.0, &mut x.1));
    /* Edges that form cycles */
    let cycles = scc::strongly_connected(&trimmed);
    /* Initially each node is labeled by its own id */
    let nodes = cycles.map_in_place(|x| x.0 = x.1.clone()).consolidate();
    /* Propagate smallest ID within SCC */
    let scclabels = propagate::propagate(&cycles, &nodes);
    scclabels.map(move |(n, l)| _scclabels(ddlog_std::tuple2(n, l)))
}

pub fn ConnectedComponents<S, V, E, N, EF, LF>(
    edges: &Collection<S, V, Weight>,
    _edges: EF,
    from: fn(&E) -> N,
    to: fn(&E) -> N,
    _cclabels: LF,
) -> (Collection<S, V, Weight>)
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    V: differential_dataflow::Data,
    N: differential_dataflow::ExchangeData + std::hash::Hash,
    E: differential_dataflow::ExchangeData,
    EF: Fn(V) -> E + 'static,
    LF: Fn(ddlog_std::tuple2<N, N>) -> V + 'static,
{
    let pairs = edges.map(move |v| {
        let e = _edges(v);
        (from(&e), to(&e))
    });

    /* Initially each node is labeled by its own id */
    let nodes = pairs.map_in_place(|x| x.0 = x.1.clone()).consolidate();
    let labels = propagate::propagate(&pairs, &nodes);
    labels.map(move |(n, l)| _cclabels(ddlog_std::tuple2(n, l)))
}

pub fn ConnectedComponents64<S, V, E, N, EF, LF>(
    edges: &Collection<S, V, Weight>,
    _edges: EF,
    from: fn(&E) -> N,
    to: fn(&E) -> N,
    _cclabels: LF,
) -> (Collection<S, V, Weight>)
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    u64: From<N>,
    V: differential_dataflow::Data,
    N: differential_dataflow::ExchangeData + std::hash::Hash,
    E: differential_dataflow::ExchangeData,
    EF: Fn(V) -> E + 'static,
    LF: Fn(ddlog_std::tuple2<N, N>) -> V + 'static,
{
    let pairs = edges.map(move |v| {
        let e = _edges(v);
        (from(&e), to(&e))
    });

    /* Initially each node is labeled by its own id */
    let nodes = pairs.map_in_place(|x| x.0 = x.1.clone()).consolidate();

    /* `propagate_at` is the same as `propagate` but schedules the work by first circulating
     * all elements with the same value of the closure.
     * We use a closure that maps small numbers to small logarithms
     * to drop the footprint for the first iterative computation.
     */
    let labels = propagate::propagate_at(&pairs, &nodes, |x| u64::from(x.clone()));
    labels.map(move |(n, l)| _cclabels(ddlog_std::tuple2(n, l)))
}

pub fn UnsafeBidirectionalEdges<S, V, E, N, EF, LF>(
    edges: &Collection<S, V, Weight>,
    _edges: EF,
    from: fn(&E) -> N,
    to: fn(&E) -> N,
    _biedges: LF,
) -> (Collection<S, V, Weight>)
where
    S: Scope,
    S::Timestamp: TotalOrder + Lattice + Ord,
    V: differential_dataflow::Data,
    N: differential_dataflow::ExchangeData + std::hash::Hash,
    E: differential_dataflow::ExchangeData,
    EF: Fn(V) -> E + 'static,
    LF: Fn(ddlog_std::tuple2<N, N>) -> V + 'static,
{
    let mins = edges.map(move |v| {
        let e = _edges(v);
        let x = from(&e);
        let y = to(&e);
        if x < y {
            (x, y)
        } else {
            (y, x)
        }
    });

    let bidirectional = mins.threshold_total(|_, count| if *count > 1 { 1 } else { 0 });
    let bidirectional = bidirectional.concat(&bidirectional.map(|(x, y)| (y.clone(), x.clone())));
    bidirectional.map(move |(n1, n2)| _biedges(ddlog_std::tuple2(n1, n2)))
}
