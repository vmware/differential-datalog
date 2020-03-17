[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
[![Build Status](https://travis-ci.com/vmware/differential-datalog.svg?branch=master)](https://travis-ci.com/vmware/differential-datalog)
[![pipeline status](https://gitlab.com/ddlog/differential-datalog/badges/master/pipeline.svg)](https://gitlab.com/ddlog/differential-datalog/commits/master)
[![rustc](https://img.shields.io/badge/rustc-1.39+-blue.svg)](https://blog.rust-lang.org/2019/11/07/Rust-1.39.0.html)

# Differential Datalog (DDlog)

DDlog is a programming language for *incremental computation*. It is well suited for
writing programs that continuously update their output in response to input changes. With DDlog,
the programmer does not need to worry about writing incremental algorithms.
Instead they specify the desired input-output mapping in a declarative manner, using a dialect of Datalog.
The DDlog compiler then synthesizes an efficient incremental implementation.
DDlog is based on [Frank McSherry's](https://github.com/frankmcsherry/)
excellent [differential dataflow](https://github.com/frankmcsherry/differential-dataflow) library.

DDlog has the following key properties:

1. **Relational**: A DDlog program transforms a set of input relations (or tables) into a set of output relations.
It is thus well suited for applications that operate on relational data, ranging from real-time analytics to
cloud management systems and static program analysis tools.

2. **Dataflow-oriented**: At runtime, a DDlog program accepts a *stream of updates* to input relations.
Each update inserts, deletes, or modifies a subset of input records. DDlog responds to an input update
by outputting an update to its output relations.

3. **Incremental**: DDlog processes input updates by performing the minimum amount of work
necessary to compute changes to output relations.  This has significant performance benefits for many queries.

4. **Bottom-up**: DDlog starts from a set of input facts and
computes *all* possible derived facts by following user-defined rules, in a bottom-up fashion.  In
contrast, top-down engines are optimized to answer individual user queries without computing all
possible facts ahead of time.  For example, given a Datalog program that computes pairs of connected
vertices in a graph, a bottom-up engine maintains the set of all such pairs.  A top-down engine, on
the other hand, is triggered by a user query to determine whether a pair of vertices is connected
and handles the query by searching for a derivation chain back to ground facts.  The bottom-up
approach is preferable in applications where all derived facts must be computed ahead of time and in
applications where the cost of initial computation is amortized across a large number of queries.

5. **In-memory**: DDlog stores and processes data in memory.  In a typical use case, a DDlog program
is used in conjunction with a persistent database, with database records being fed to DDlog as
ground facts and the derived facts computed by DDlog being written back to the database.

    At the moment, DDlog can only operate on databases that completely fit the memory of a single
    machine. Se are working on a distributed version of DDlog that will be able to
    parition its state and computation across multiple machines.

6. **Typed**: In its classical textbook form Datalog is more of a mathematical formalism than a
practical tool for programmers.  In particular, pure Datalog does not have concepts like types,
arithmetics, strings or functions.  To facilitate writing of safe, clear, and concise code, DDlog
extends pure Datalog with:

    1. A powerful type system, including Booleans, unlimited precision integers, bitvectors, floating point numbers, strings,
    tuples, tagged unions, vectors, sets, and maps. All of these types can be
    stored in DDlog relations and manipuated by DDlog rules.  Thus, with DDlog
    one can perform relational operations, such as joins, directly over structured data,
    without having to flatten it first (as is often done in SQL databases).

    2. Standard integer, bitvector, and floating point arithmetic.

    3. A simple procedural language that allows expressing many computations natively in DDlog without resorting to external functions.

    4. String operations, including string concatenation and interpolation.

    5. Syntactic sugar for writing imperative-style code using for/let/assignments.

7. **Integrated**: while DDlog programs can be run interactively via a command line interface, its
primary use case is to integrate with other applications that require deductive database
functionality.  A DDlog program is compiled into a Rust library that can be linked against a Rust,
C/C++, Java, or Go program (bindings for other languages can be easily added).  This enables good performance,
but somewhat limits the flexibility, as changes to the relational schema or rules require re-compilation.

## Documentation

- Follow the [tutorial](doc/tutorial/tutorial.md) for a step-by-step introduction to DDlog.
- DDlog [language reference](doc/language_reference/language_reference.md).
- [Instructions](doc/testing/testing.md) for writing and testing your own Datalog programs.
- [How to](doc/java_api.md) use DDlog from Java.
- [How to](go/README.md) use DDlog from Go and [Go API documentation](https://pkg.go.dev/github.com/vmware/differential-datalog/go/pkg/ddlog).
- [DDlog overview paper](doc/datalog2.0-workshop/paper.pdf), Datalog 2.0 workshop, 2019.

## Installation

### Installing DDlog from a binary release

To install a precompiled version of DDlog, download the [latest binary release](https://github.com/vmware/differential-datalog/releases), extract it from archive and add `ddlog/bin` to your `$PATH`. You will also need to install the Rust toolchain (see instructions [below](#prerequisites))

You are now ready to [start coding in DDlog](doc/tutorial/tutorial.md).

### Compiling DDlog from sources

#### Installing dependencies manually

- Haskell [stack](https://github.com/commercialhaskell/stack):
  ```
  wget -qO- https://get.haskellstack.org/ | sh
  ```
- Rust toolchain v1.39 or later:
  ```
  curl https://sh.rustup.rs -sSf | sh
  . $HOME/.cargo/env
  rustup component add rustfmt
  rustup component add clippy
  ```
  **Note:** The `rustup` script adds path to Rust toolchain binaries (typically, `$HOME/.cargo/bin`)
  to `~/.profile`, so that it becomes effective at the next login attempt.  To configure your current
  shell run `source $HOME/.cargo/env`.
- JDK, e.g.:
  ```
  apt install default-jdk
  ```
- Google FlatBuffers library.  Download and build FlatBuffers release 1.11.0 from
  [github](https://github.com/google/flatbuffers/releases/tag/v1.11.0).  Make sure
  that the `flatc` tool is in your `$PATH`.  Additionally, make sure that FlatBuffers
  Java classes are in your `$CLASSPATH`:
  ```
  ./tools/install-flatbuf.sh
  cd flatbuffers
  export CLASSPATH=`pwd`"/java":$CLASSPATH
  export PATH=`pwd`:$PATH
  cd ..
  ```
- Static versions of the following libraries: `libpthread.a`, `libc.a`, `libm.a`, `librt.a`, `libutil.a`,
  `libdl.a`, `libgmp.a` can be installed from distro-specific packages.  On Ubuntu:
  ```
  apt install libc6-dev libgmp-dev
  ```

#### Installing dependencis using Nix

Alternativaly, [Nix](https://nixos.org/nix/) package manager provides an automated way to
install and manage the above dependencies.  It works on many flavors of
Linux and MacOS.

1. Get Nix https://nixos.org/nix/
2. Clone this repo and cd into it
3. Run Nix dev shell: `nix-shell`
4. Inside dev shell run `initial-setup` to build `ddlog` executable. This should be done only once

#### Building

To rebuild the software once you've installed the dependencies using one of the
above methods:

```
stack build
```

To install DDlog binaries in Haskell stack's default binary directory:

```
stack install
```

To install to a different location:

```
stack install --local-bin-path <custom_path>
```

To test basic DDlog functionality (complete test suite takes a long time):

```
stack test --ta '-p path'
```

**Note:** this takes a few minutes

You are now ready to [start coding in DDlog](doc/tutorial/tutorial.md).

### vim syntax highlighting

The easiest way to enable differential datalog syntax highlighting for `.dl` files in Vim is by
creating a symlink from `<ddlog-folder>/tools/vim/syntax/dl.vim` into `~/.vim/syntax/`.

If you are using a plugin manager you may be able to directly consume the file from the upstream
repository as well. In the case of [`Vundle`](https://github.com/VundleVim/Vundle.vim), for example,
configuration could look as follows:

```vim
call vundle#begin('~/.config/nvim/bundle')
...
Plugin 'vmware/differential-datalog', {'rtp': 'tools/vim'} <---- relevant line
...
call vundle#end()
```

## Debugging with GHCi

To run the test suite with the GHCi debugger:

```
stack ghci --ghci-options -isrc --ghci-options -itest differential-datalog:differential-datalog-test
```

and type `do main` in the command prompt.

## Building with profiling info enabled

```
stack clean
```

followed by

```
stack build --profile
```

or

```
stack test --profile
```
