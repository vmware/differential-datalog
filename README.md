# Differential Datalog

An in-memory incremental Datalog engine based on Differential Dataflow.
This is currently very early work-in-progress.

# Requirements

We have only tested our software on Ubuntu Linux.

The compilers are written in Haskell.  One needs the Glasgow Haskell
Compiler.  The Haskell compiler is managed by the
[stack](https://github.com/commercialhaskell/stack) tool.

To download stack (if you don't already have it) you can use:

```
wget -qO- https://get.haskellstack.org/ | sh
```

# Building

To build the software execute:

```
stack build
```

To run the tests execute:

```
stack test
```

# Using IntelliJ IDEA

You can download and install the community edition of IntelliJ IDEA
from https://www.jetbrains.com/idea/download.

Start IntelliJ.

Install the IntelliJ-Hakell plugin: (File/Settings/Plugins -- search).

The first time: open the project: File/New/Project from existing sources, select
the top-level folder.

For project SDK choose "Haskell Tool Stack".

You may need to change the project settings: (File/Project/Structure):

- change the project SDK to be Haskell
- change the platform SDK to be Haskell



