# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased

### SQL-to-DDlog compiler

- Bug fixes and improvements in SQL-to-DDlog compiler.
- Enable SQL-to-DDlog compiler to translate `array_length` function calls, which appear in SQL dialects such as H2 and Postgres.

### New features

- Added change profiling support to the DDlog self-profiler.  Unlike arrangement size
  profiling, which tracks the number of records in each arrangment, the change profile
  shows the amount of churn.  For example adding one record and deleting one record will
  show up as two changes in the change profile, but will cancel out in the size profile.
  The self-profiler now support the `profile change on/off` commands (also available
  through the API), which enables change profiling for selected transactions.
  When change profiling is disabled, the recording stops, but the previously accumulated
  profile is preserved.  By selectively enabling change profiling for a subset of transactions,
  the user can focus their analysis on specific parts of the program.

- Limited support for dynamic typing.  We introduce a new type `Any` to the standard
  library, which can represent any DDlog value, along with two library functions
  `to_any()` and `from_any()` that convert values to and from this type.  This
  feature can be used to, e.g., store a mix of values of different types in a
  set or map.

- Experimental features to support implementing parts of D3log runtime in DDlog.
  See #1065 for details.

### Language and standard library changes

- The semantics of the `group_by` operator changed in a subtle way.  A group
  now contain exactly one occurrence of each value. See #1070 for details.

- Removed `ddlog_std::count(Group)` and `ddlog_std::group_count(Group)` methods
  to avoid changing their behavior in a non-backwards-compatible way.  Added
  `count_distinct(Group)` instead, which returns the count of distinct values in
  the group.

- Removed `ddlog_std::group_sum(Group)`.  Added
  `function sum_of(g: Group<'K, 'V>, f: function('V): 'N): 'N` instead.

## [0.47.0] - Aug 19, 2021

### OVSDB-to-DDlog compiler update

- Added `--intern-strings` option that causes all strings in generated OVSDB tables
  to be emitted as 'istring'.  This reduces memory use and can aid performance in programs
  that use strings heavily.  See PR #1056.

## [0.46.0] - Aug 18, 2021

### Optimizations

- Speedup hashing of interned objects (fixes performance regression in 0.43.0,
  see #1053 for details).
- Speedup serialization of the `json::JsonValue` type (see #1052)

### Initial Calcite SQL support

- Added partial support for the Calcite dialiect of SQL (see #1044) to the
  SQL-to-DDlog translator.


## [0.45.1] - Aug 16, 2021

### Optimizations

- Optimize code generation for `match` and `?` expressions.
- Optimize code generation for interpolated strings.

## [0.45.0] - Aug 15, 2021

### Optimizations

- Introduce [`#[by_val]`](https://github.com/vmware/differential-datalog/blob/master/doc/tutorial/tutorial.md#by_val)
  attribute to pass function arguments by value.
- Use the new attribute to optimize a bunch of library functions.  This should
  not break any existing DDlog code, but it will affect Rust code that calls
  DDlog libraries directly.

## [0.44.0] - Aug 12, 2021

### Optimizations

- Compile string literals into lazy static to avoid dynamic allocation every
  time a string literal is used.

### Bug fixes

- Bug fixes in the SQL-to-DDlog compiler
- Bug in `#[derive(Mutator)]` macro: mutations that change the constructor of a
  type failed (#1041).

### Improvements

- Improved infrastructure for implementing `FromRecord` and `Mutator` traits
  (#1029):
  - Automatically handle `Record::Serialized()` in `FromRecord` and `Mutator`
    implementations.
  - Allow modifying, and not just overwriting `Map` values.

## [0.43.0] - Jul 25, 2021

### Bug fixes
  - Fixed a bug in type inference: #1022.
  - Fixed non-deterministic behavior in `internment.dl`: e0be732061e2556b0bbbfaceb0ab04a76f573ec8

### New library functions

- New `ddlog_std` library functions:

    ```
    /* Convert any DDlog type into a string in a programmer-facing,
     * debugging context.  Implemented by calling the `Debug::fmt()`
     * method of the underlying Rust type. */
    extern function to_string_debug(x: 'T): string

    function reverse(v: mut Vec<'X>)
    function reverse_imm(v: Vec<'X>): Vec<'X>
    ```

## [0.42.1] - Jul 16, 2021

- New library functions:

  - `ddlog_std.dl`:
    ```
    function values(m: Map<'K, 'V>): Vec<'V>
    function nth_value(m: Map<'K, 'V>, n: usize): Option<'V>
    function nth_key(m: Map<'K, 'V>, n: usize): Option<'K>
    ```

  - `map.dl`:
    ```
    function find(m: Map<'K, 'V>, f: function('V): bool): Option<'V>
    function any(m: Map<'K, 'V>, f: function('V): bool): bool
    ```

- Bug fixes:

  - Fixed scrambled self-profiler output.
  - Fixed compilation speed regression introduced in 0.42.0.

- New feature in OVSDB-to-DDlog compiler:

  - `multiset-table` option to force an output-only table to be declared as a
    `multiset`.

## [0.42.0] - Jul 9, 2021

### DDShow integration

- [DDShow](https://github.com/Kixiron/ddshow/) is a timely/differential dataflow
  profiler developed by @Kixiron.  DDlog now supports DDShow as an alternative
  to its built-in profiler.  The built-in profiler is still preferable in
  production environments, due to its low overhead, but DDShow is a better (and
  constantly improving!) option for development-time profiling.  See
  [Profiling tutorial](doc/profiling.md) for details on how to enable
  DDShow-based profiling via CLI switches, as well as via Rust/C/Java APIs.

### Configuration API

- DDlog now supports several configuration parameters: (1) number of worker threads,
  (1) idle merge effort, (3) profiling configuration, (4) debug regions.  We
  introduce a new startup API `<your_program>_ddlog::run_with_config()` that allows
  the user to configure these parameters before instantiating a DDlog program.  This
  API is exposed to C via the `ddlog_run_with_config()` function and to Java via
  the `DDlogConfig` class.

### Breaking API changes

- Rust API refactoring: Moved `HDDlog` type (handle to a running DDlog program) from
  the main auto-generated crate to the `differential_datalog` crate.  The
  auto-generated crate now exports two public functions that create an `HDDlog`
  instance: `run` and `run_with_config`.

- Profiling is disabled by default.  Previously, DDlog's self-profiler was
  always enabled by default.  This is no longer the case.  When starting the
  program via `<your_program>_ddlog::run()` (`ddlog_run()` in C), the profiler
  will be disabled.  To enable the profiler, use the `run_with_config()` API
  described above and explicitly set the profiling mode to either `SelfProfiling`
  (to enable the internal profiler) or `TimelyProfiling` (to use DDShow).

### New features

- Support for "original" annotation on relations to record original name
  (if name is generated by code).

### Bug fixes

- Link ovsdb2ddlog statically, making it self-contained for better portability
  across different Linux distros.


## [0.41.0] - Jun 17, 2021

- Addressed some warnings from rustc 1.52+.
- New release process: we now use GitHub actions instead of Travis to create
  binary DDlog releases.  This should not have any effect on users.

## [0.40.3] - Jun 12, 2021

### Optimization

- Made all reference counting (internal and through `ddlog_std::Ref`) use reference counters without weak counts
  in an effort to reduce memory usage

### Bug fixes

- Fixed compilation error in Go bindings [#993]
- Support building ddlog from a source tarball outside of a git repo [#986]
- ovsdb2ddlog: Support negative values in OVS schemas [#985]
- More string conversions from utf-8 and utf-16
- print/debug functions

## [0.40.2] - May 11, 2021

### Libraries

- `time.dl`: Improved support for times and dates.  The library now uses the
  `chrono` crate (instead of `time`) internally, which in particular supports
  timezones.

### Bug fix

- Segfault in the Java API in `transactionCommitDumpChanges`.

## [0.40.1] - May 7, 2021

### Libraries

- `ddlog_std.dl`: More string conversions from utf-8 and utf-16.
- `print.dl`: print/debug functions.

### Bug fix

- Rust compilation error due to missing parens in generated code.

## [0.40.0] - April 29, 2021

### Changes

- Upgraded to timely dataflow and differential dataflow dependencies to v0.12.
- Worked on improving the debuggability of ddlog dataflow graphs

## [0.39.0] - April 11, 2021

### D3log

- Experimental compiler support for D3log (wip).

### New features

- We now support rules that don't start with a positive literal, e.g.,
  ```
  R() :- not R2(...).
  R() :- var x = 5.
  ```

### Bug fix

- Delete old Rust files in the generated project.  This prevents compilation
  errors when upgrading to a new version of DDlog.

## [0.38.0] - Mar 11, 2021

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
