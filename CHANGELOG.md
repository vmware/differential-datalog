# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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
