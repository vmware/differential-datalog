# DDlog command language

This document describes the text format used to issue commands to DDlog programs
via the CLI interface.  Set [tutorial](../tutorial/tutorial.md) for more details on
compiling and running DDlog programs.

## Command reference


| Command                        | Example                                          | Description                                                            |
| ------------------------------ |--------------------------------------------------| -----------------------------------------------------------------------|
| `start;`                       |                                                  | start a transaction                                                    |
| `commit;`                      |                                                  | commit current transaction                                             |
| `commit dump_changes;`         |                                                  | commit current transaction and dump all changes to output relations    |
| `rollback;`                    |                                                  | rollback current transaction; reverting all changes                    |
| `timestamp;`                   |                                                  | print current time in ns since some unspecified epoch                  |
| `dump;`                        |                                                  | dump the content of all output relations                               |
| `dump <relation>;`             | `dump Rel1;`                                     | dump the content of an individual output relation                      |
| `query_index <index>(<args>);` | `query_index Edge_by_from(100);`                 | dump all values in an indexed relation with the given key              |
| `dump_index <index>;`          | `dump_index Edge_by_from;`                       | dump all values in an indexed relation                                 |
| `echo <text>;`                 | `echo Hello world;`                              | copy arbitrary text to stdout                                          |
| `log_level <level>;`           | `log_level 100000;`                              | set maximum log level for messages output via log API; messages with higher priority will be dropped (see [log.dl](../..//lib/log.dl)) |
| `insert <record>,`             | `insert Rel1(1,true,"foo");`                     | insert record to relation Rel1                                         |
|                                | `insert Rel1(.arg2=true,.arg1=1, .arg3="foo");`  | as above, but uses named rather than positional arguments              |
|                                | `insert Rel2(.x=10, .y=Constructor{"foo", true});` | passing structured data by calling type constructor                  |
|                                | `insert Rel2(.x=10, .y=Constructor{.f1="foo", .f2=true});` | type constructor arguments can also be passed by name        |
| `insert_or_update <record>,`   | same as `insert`, but replaces existing value with the same key, if it exists; only valid for relations with primary key  |
| `delete <record>,`             | `delete Rel1(1,true,"foo");`                     | delete record from Rel1 (using argument syntax identical to `insert`)  |
| `delete_key <relation> <key>,` | `delete Rel1 1;`                                 | delete record by key; only valid for relations with primary key        |
| `modify <relation> <key> <- <record>,` | `modify Rel1 1 <- Rel1{.f1 = 5};`        | modify record; `<record>` specifies just the fields to be modified; only valid for relations with primary key        |
| comma-separated updates        | `insert Foo(1), delete Bar("buzz");`             | a sequence of insert and delete commands can be applied in one update  |
| `profile`                      |                                                  | print CPU and memory profile of the DDlog program                      |
| `profile cpu "on"/"off"`       |                                                  | controls the recording of differential operator runtimes; set to "on" to enable the construction of the programs CPU profile (default: "off") |
| `exit;`                        |                                                  | terminates execution                                                   |
| `#`                            | `# comment ending at the end of line`            | comment that ends at the end of the line                               |

**Note**: Placing a semicolon after an `insert` or `delete` operation tells DDlog to apply the
update instantly, without waiting for subsequent updates.  Comma-separated updates are only applied
once the full list of updates has been read (end of list is indicated by a semicolon).  This is
significanly more efficient and should always be the preferred option when the client wants to apply
multiple updates that are known at the same time.

## String literals

DDlog command syntax supports two forms of string literals:

- Simple quoted string literals, e.g., `"foo"`,
- String literals imported from files.  This form is convenient when working
  with long strings.  Such literals consist of a file name
  prefixed by the percent symbol: `%"<filename>"`, e.g.,
  `insert Rel1(1,true,%"file.txt");`, where `file.txt` is a file containing
  arbitrary UTF-8 text.

## On-the-fly deserialization

DDlog relies on the Rust `serde` crate to generate serialization and
deserialization methods for all DDlog types.  This feature can be
used to feed serialized data to DDlog, deserializing it on the fly.  Serialized
values are represented in the command file in the following format:
`@<encoding><string_literal>`, where `<encoding>` is the data encoding
(currently the only supported value is `json`), and `<string_literal>` is the
string containing serialized representation of the value in this encoding.
For instance, the following command inserts a record to relation `Rel`, initializing
its field `x` from a JSON string string stored in file `filename.json`

```
insert Rel(.x = @json%"filename.json"),
```

## Example workload `path.dat`

```
start;

insert Edge("Palo Alto", "Palo Alto"),
insert Edge("Palo Alto", "Redwood City"),
insert Edge("Redwood City", "San Bruno");

commit;
dump;
```
