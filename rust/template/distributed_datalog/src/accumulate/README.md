# D3log Accumulator
## Rationale
To increase fault-tolerance and allow dynamic re-configuration in a distributed setting, a possible approach is to keep 
track of relation updates received and sent by nodes in an efficient accumulated manner. Upon node failure or addition 
of a new sink to the network, a single transaction including all accumulated updates then suffices to have a consistent 
state in the network.

## Design
An Accumulator implements the `Accumulator` trait, which is an `Observer` and `Observable` simultaneously. 
The `DistributingAccumulator` is a subtype that observes a single observable and can be by multiple observers.

Conceptually, the `DistributingAccumulator` consists of two different components:
the `AccumulatingObserver` and a `TxnDistributor`. It encapsulates the functionality
of those components in a single structure.

The `AccumulatingObserver` is a simple proxy between an observable and an observer that
inspects the data that passes through. It accumulates the data to keep track of the
current state. The data structure used for aggregating the updates is a simple `HashMap<RelId, HashSet<V>>`, 
that maps each relation to a set of values currently included in it. This assumes that the Accumulator always 
starts with an empty set and should never accumulate negative multiplicities.

The `TxnDistributor` is the inverse of the `TxnMux` class, it listens to a single observable and
is able to send data to multiple observers. 

The following schema visualizes the relationship between the different components:
```
                   +----------------------------------------------------------------------+                     
                   |                                                                      |                     
                   |                      DistributingAccumulator                         |                     
                   |                                                                      |   /--Observer       
                   |  +----------------------+                 +---------------------+  /-|---                  
                   |  |                      |                 |                     ---  |                     
 Observable -------|---AccumulatingObserver  |-----------------|  TxnDistributor     -----|------Observer       
                   |  |                      |                 |                     | \--|                     
                   |  |                      |                 |                     |    |---                  
                   |  +----------------------+                 +---------------------+    |   \- Observer       
                   |                                                                      |                     
                   |                                                                      |                     
                   +----------------------------------------------------------------------+     
 ```

When a component subscribes to `DistributingAccumulator`, the accumulator acquires a lock for the `TxnDistributor`, 
collects the accumulated updates from `AccumulatingObserver` and sends it to the component and then subscribes the 
component to `TxnDistributor`, releasing the lock afterwards.

### Future Development
- Currently, `TcpReceiver` encapsulates multiple input connections from other nodes, and there is only a single 
  accumulator in place for all incoming TCP connections.
  Accumulators for each single node source cannot be created in `instantiate.rs` and managed by the `Realization`
  struct but would need to be managed by `TcpReceiver`. A more intuitive approach would be to let `Realization` manage 
  each of the node source connections and the corresponding accumulators, thus, refactoring `TcpReceiver`.
  
- `instantiate.rs` should have a `Realization` struct with methods implemented on it instead of multiple
  static methods (`add_file_sources`, etc.) where each method call needs a lot of data objects handed over.
  
- The Observer-Observable-Interface probably needs an additional method (e.g., `on_failure`) that signals to an 
  accumulator that the source stopped unexpectedly and it should issue deletion updates for the accumulated state
  to its sink. Currently, `on_completed` is used for this. However, this is probably not the right design, 
  as it conflates the notions that a data source will not produce any more data and that the data source 
  has been removed and should be considered empty.
  