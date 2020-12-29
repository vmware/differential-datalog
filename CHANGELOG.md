# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### API changes

- Removed callback argument from `HDDlog::run`, `ddlog_run`, and Go/Java language
  bindings based on `ddlog_run`.  This optional callback, invoked by DD workers on
  each update to an output collection complicated the API and was tricky to use
  correctly.  Most importantly, it is superseded by the `commit_dump_changes` API.

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
