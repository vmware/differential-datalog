# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### API changes

There are some breaking API changes in this release:

- Rust API:
  - Factored the Rust API into several traits declared in the
    `differential_datalog` crate:
      - `trait DDlogDynamic` - works with data represented as records.
      - `trait DDlog` - extends `DDlogDynamic` to work with strongly typed
         values wrapped in `DDValue`.
      - `trait DDlogProfiling` - profiling API.
      - `trait DDlogDump` - dump tables and indexes.
      - `DDlogInventory` - convert between relation/index names and numeric ids.
  - Renamed `apply_valupdates` -> `apply_updates`,
            `apply_updates` -> `apply_updates_dynamic`.
  - Changed method signatures to eliminate any generics.  This way we will
    be able to implement dynamic dispatch for the DDlog API (i.e., pass
    references to a DDlog program as `&dyn DDlogXXX`) in the future.

- C, Java, Go API.  `ddlog_get_table_id`, `ddlog_get_index_id` methods now
  require a DDlog instance, e.g., old signature:
  ```
  extern table_id ddlog_get_table_id(const char* tname);
  ```
  new signature:
  ```
  extern table_id ddlog_get_table_id(ddlog_prog hprog, const char* tname);
  ```

### Libraries

- Functional HashSets, aka immutable hashsets (`lib/hashset.dl`).  At the API
  level functional hashsets behave just like regular hashsets; however their
  internal implementation supports cloning a hashset in time O(1) by sharing the
  entire internal state between the clone and the parent.  Modifying the clone
  updates only the affected state in a copy-on-write fashion, with the
  rest of the state still shared with the parent.

  Example use case: computing the set of all unique id's that appear in a
  stream.  At every iteration, we add all newly observed ids to the set of
  id's computed so far.  This would normally amount to cloning and modifying a
  potentially large set in time `O(n)`, where `n` is the size of the set.  With
  functional sets, the cost if `O(1)`.

  Functional data types are generally a great match for working with immutable
  collections, e.g., collections stored in DDlog relations.  We therefore plan
  to introduce more functional data types in the future, possibly even
  replacing the standard collections (`Set`, `Map`, `Vec`) with functional
  versions.

### ovsdb2ddlog compiler

- Added `--intern-table` flag to the compiler to declare input tables coming from
  OVSDB as `Intern<...>`.  This is useful for tables whose records are copied
  around as a whole and can therefore benefit from interning performance- and
  memory-wise.  In the past we had to create a separate table and copy records
  from the original input table to it while wrapping them in `Intern<>`.  With
  this change, we avoid the extra copy and intern records as we ingest them
  for selected tables.


## [0.37.1] - Feb 23, 2021

### Optimizations

- Internal refactoring to improve DDlog's scalability with multiple worker
  threads.

### Documentation

- [C API tutorial](doc/c_tutorial/c_tutorial.rst), kindly contributed by @smadaminov.

### Bug fix

- Compiler crashed when differentiation or delay operators were applied to
  relations declared outside of the main module of the program.

## [0.37.0] - Feb 15, 2021

### Language improvements

- Enable pattern matching in `for` loops and `FlatMap`. One can now write:
    ```
    for ((k,v) in map) {}
    ```
    instead of
    ```
    for ((kv in map) { var k = kv.0}
    ```
    and
    ```
    (var x, var y) = FlatMap(expr)
    ```
    instead of:
    ```
    var xy = FlatMap(expr),
    var x = xy.0
    ```

- Remove the hardwired knowledge about iterable types from the compiler.
  Until now the compiler only knew how to iterate over `Vec`, `Set`,
  `Map`, `Group`, and `TinySet` types.  Instead we now allow the programmer
  to label any extern type as iterable, meaning that it implements `iter()`
  and `into_iter()` methods, that return Rust iterators using one of two
  attributes:
  ```
  #[iterate_by_ref=iter:<type>]
  ```
  or
  ```
  #[iterate_by_val=iter:<type>]
  ```
  where `type` is the type yielded by the `next()` method of the iterator
  The former indicates that the `iter()` method returns a by-reference
  iterator, the latter indicates that `iter()` returns a by-value
  iterator.

  As a side effect of this change, the compiler no longer distinguishes
  maps from other cotnainers that over 2-tuples.  Therefore maps are
  represented as lists of tuples in the Flatbuf-based Java API.

- Groups now iterate with weight.  Internally, all DDlog relations are
  multisets, where each element has a weight associated with it.  We change
  the semantics of groups to expose these weights during iteration.
  Specifically, when iterating over a group in a for-loop or flattening it
  with `FlatMap`, each value in the iterator is a `(v, w)` tuple, where `w`
  is the weight of element `v`.

  We keep the semantics of all existing library aggregates unchanged,
  i.e., they ignore weights during iteration.  This means that most existing
  user code is not affected (custom aggregates are not that common).

- The `group_by` operator now works on streams and produces a stream of groups,
  one for each individual transaction.
  ([Tutorial section](doc/language_reference/language_reference.md#streaming-aggregation-differentiation-and-delay-operators))

- Two new DDlog operators: **delay** and **differentiation**.  The former refers
  to the contents of a relation from `N` transactions ago, where `N` is a positive
  integer constant.  The latter converts a stream into a relation that contains new
  values added to the stream byt the last transaction.
  ([Tutorial section](doc/language_reference/language_reference.md#streaming-aggregation-differentiation-and-delay-operators))

## [0.36.0] - Feb 7, 2021

### API changes

- Support insert_or_update and delete_by_key in flatbuf API, including in Java.

### Other improvements

- Introduced a benchmarking framework for DDlog (see rust/ddlog_benches/README.md).
- Print compiler error messages so that emacs can parse the error location.

## [0.35.0] - Jan 19, 2021

### Optimizations

- Use `internment` crate instead of `arc-interner` to make `Intern<T>`
  more scalable.

## [0.34.2] - Jan 18, 2021

### New features

- Support for [ephemeral streams](doc/language_reference/language_reference.md#streams)

## [0.34.1] - Jan 17, 2021

### Bug fixes

- Fix linker problem on MacOS.

### Improvements

- A heuristic to make type inference errors easier to understand.

### Libraries

New functions in `internment.dl`:

    ```
    function parse_dec_u64(s: istring): Option<bit<64>>
    function parse_dec_i64(s: istring): Option<signed<64>>
    ```

## [0.34.0] - Jan 11, 2021

### Bug fixes

- A change in serde caused DDlog-generated Rust code to stop compiling,
  affecting all recent DDlog releases.

### API changes

- Removed callback argument from `HDDlog::run`, `ddlog_run`, and Go/Java language
  bindings based on `ddlog_run`.  This optional callback, invoked by DD workers on
  each update to an output collection complicated the API and was tricky to use
  correctly.  Most importantly, it is superseded by the `commit_dump_changes` API.

### Added

- Added the `ddlog_derive` crate that provides derive macros for the `FromRecord`,
  `IntoRecord` and `Mutator` traits
- Added the `Record::positional_struct_fields()` method to allow fetching positional
  fields from records
- Added the `Record::get_struct_field()` method to allow getting a struct record's
  field by name

## [0.33.0] - Dec 24, 2020

### Optimizations

- An optimized implementation of the `distinct` operator may save memory and CPU
  for recursive relations.

### Libraries

- Added support for regex sets to `lib/regex.dl`
- Added `Vec::pop()` function to `lib/ddlog_std.dl`.

### Miscellaneous improvements:

- Upgrade to the latest versions of timely and differential dataflow crates.

## [0.32.1] - Dec 22, 2020

### Optimizations

- Sped-up the compiler: eliminated several performance bottlenecks, most notably
  in the type inference algorithm.  This yields a 10x speedup on large DDlog
  projects.

### Bug fixes

- Fixed regressions introduced in 0.32.0: #859, #860.

[0.33.0]: https://github.com/vmware/differential-datalog/releases/tag/v0.33.0
[0.32.1]: https://github.com/vmware/differential-datalog/releases/tag/v0.32.1
