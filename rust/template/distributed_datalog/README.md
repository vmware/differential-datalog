Distributed Differential-Datalog
----------------------------------------
This crate contains most of the functionality comprising **Distributed
Differential-Datalog** (**D3log**). As the name suggests, **D3log** is
an effort to connect multiple `differential-datalog` programs in a
sensible way and distribute their computation among multiple compute
nodes.

### *Note:*
----------------------------------------------------------------------
> **D3log** is a work in progress with the API surface constantly
> evolving and changing. No backwards compatibility is provided.
----------------------------------------------------------------------


### Organization/Structure
**D3log** is being developed against and made accessible via the Rust
APIs of a `ddlog` project, with all the code residing below
[`rust/template/`][rust-template]. The current design is based on a
variation of the observer pattern as employed and described by
[ReactiveX][reactivex.io], with a program running on one node (an
"observer) subscribing to (i.e., observing) the changes produced by
another (an "observable").

**D3log** is being built bottom up, starting with the individual
building blocks we envision to use in the future in a well tested form.
As it stands that means we have the following components:
- the `Observable`-`Observer` core-infrastructure itself, residing in
  the [`observe`][observe] crate
- a `DDlogServer` (residing in [`server.rs`][server.rs] that wraps a
  standard `HDDlog` and adapts it to the `Observer` interface
  - such an object can also stream changes produced by certain (output)
    relations and these changes can be subscribed to (i.e., such a
    stream is an instance of an `Observable`)
- a TCP channel (contained in the [`tcp_channel`][tcp_channel] crate)
  comprised of a `TcpSender` (an `Observer`) and a `TcpReceiver` (an
  `Observable) that can connect two `ddlog` programs (wrapped in
  `DDlogServer` objects) and transfer deltas produced

### Examples & Tests
**D3log** has unit as well as integration style tests using an actual
`ddlog` program that serve both as tests and examples on how to use the
APIs. Unit tests reside along with the individual components (in this or
dependent crates inside of it) and integration tests are located in the
[`server_api`][server_api] program.


[observe]: observe
[reactivex.io]: http://reactivex.io/documentation/observable.html
[rust-template]: ..
[server.rs]: ../src/server.rs
[server_api]: ../../../test/datalog_tests/server_api
[tcp_channel]: tcp_channel
