# Creating Datalog Tests

**Thank you for contributing to the differential datalog project!**

Follow these steps, detailed below, to create a datalog test.  Note 
that this testing method is primarily suitable for testing correctness 
of the datalog engine.  It is not suitable for performance or memory 
testing, which requires feeding a large number of records to datalog.

1. Write a test datalog program
1. Create test workload
1. Create a pull request

## Writing a test program

Test files are located in the `test/datalog_tests` directory.  The
test script will automatically test all files with `*.dl`
extension.  

1. Create a file named `test/datalog_tests/<test>.dl`, where `<test>` is
   the name of your test.
1. Create your test program, including type declarations, relations, functions, and rules.
1. Run `stack test` (or `stack test --ta '-p <test>'` to run an individual test only) anywhere in the source tree.  The test script will
   automatically validate and compile the program.  Fix any
   compilation errors.  If you get a rust compilation error message starting with
something like `cargo test failed with exit code ExitFailure 101`,
this means that the datalog compiler produced invalid Rust code.  Please
[submit a bug report](https://github.com/ryzhyk/differential-datalog/issues).
*Note: compilation will take a minute or two the first time as cargo pulls and compiles Rust dependencies*.
Once compilation succeeds, it will create
`test/datalog_test/<test>.ast` file and a golden test file `test/datalog_test/<test>.ast.expected`.
Remove this file whenever you change the datalog program to re-generate it.  

### Example datalog program `path.dl`

The following example computes all paths in a graph as a transitive closure of the edge relation.

```
typedef node = string

ground relation Edge(s: node, t: node)

relation Path(s1: node, s2: node)

Path(x, y) :- Edge(x,y).
Path(x, z) :- Path(x, w), Edge(w, z).
```

## Creating test workload

Next we would like to check that the datalog program computes correct
outputs.

1. Create a file named `test/datalog_tests/<test>.dat` (the test script
   will automatically associate this file with the corresponding `.dl` file).
1. Populate the file with a sequence of commands from the table below.
1. Run `stack test` again.  This should create a file named
   `test/datalog_tests/<test>.dump` containing the output of all `dump` and `echo` commands in the script
   and `test/datalog_tests/<test>.dump.expected`.
1. Inspect the dump file; make sure that the output of the tool is correct. If it's not, 
   please [submit a bug report](https://github.com/ryzhyk/differential-datalog/issues) 
   including `.dl` and `.dat` files.
1. Make sure that you delete the `.expected` file whenever the datalog
   program or the test workload changed; otherwise the test will fail.

| Command                | Example                           | Description                                        |
| ---------------------- |-----------------------------------| ---------------------------------------------------|
| `start;`               |                                   | start a transaction                                |
| `commit;`              |                                   | commit current transaction                         |
| `rollback;`            |                                   | rollbak current transaction; reverting all changes |
| `dump;`                |                                   | dump the content of all relations                  |
| `echo <text>;`         | echo Hello world;                 | copies arbitrary text to stdout                    |
| `insert <record>;`     | insert Rel1(1,true,"foo");        | inserts record to relation Rel1                    |
| `delete <record>;`     | delete Rel1(1,true,"foo");        | deletes record to relation Rel1                    |
| comma-separated updates| insert Foo(1), delete Bar("buzz");| a sequence of insert and delete commands can be applied on one update|

### Example workload `path.dat`

```
start;

insert Edge("Palo Alto", "Palo Alto"),
insert Edge("Palo Alto", "Redwood City"),
insert Edge("Redwood City", "San Bruno");

commit;
dump;
```

This workload generates the following `path.dump` file:

```
Path:
Path{"Palo Alto","Redwood City"}
Path{"Palo Alto","Palo Alto"}
Path{"Palo Alto","San Bruno"}
Path{"Redwood City","San Bruno"}

Edge:
Edge{"Palo Alto","Redwood City"}
Edge{"Palo Alto","Palo Alto"}
Edge{"Redwood City","San Bruno"}
```

## Creating a pull request.

Include the following files in your PR:

1. `<test>.dl`
1. `<test>.ast.expeted`
1. `<test>.dat`
1. `<test>.dump.expected`
