# Differential Datalog

An in-memory incremental Datalog engine based on Differential Dataflow.
This is currently very early work-in-progress.

Language reference for the Datalog dialect implemented by the tool
can be found at [here](doc/language_reference/language_reference.md).

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

Follow the instructions here: https://github.com/rikvdkleij/intellij-haskell to 
get started with the plugin.

# Debugging with GHCi

To run the test suite with the GHCi debugger:

```
stack ghci --ghci-options -isrc --ghci-options -itest differential-datalog:differential-datalog-test
```

and type `do main` in the command prompt.

# Building with profiling info enabled

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
