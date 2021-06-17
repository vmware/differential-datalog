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
| `timestamp;`                   |                                                  | print current time in ns since the start of the program's execution    |
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
| `delete_key <relation> <key>,` | `delete_key Rel1 1;`                             | delete record by key; only valid for relations with primary key        |
| `modify <relation> <key> <- <record>,` | `modify Rel1 1 <- Rel1{.f1 = 5};`        | modify record; `<record>` specifies just the fields to be modified; only valid for relations with primary key.  See [below](#modify_command) for more details. |
| comma-separated updates        | `insert Foo(1), delete Bar("buzz");`             | a sequence of insert and delete commands can be applied in one update  |
| clear <relation>               | `clear Foo`                                      | remove all records from a relation; must be used within a transaction  |
| `profile`                      |                                                  | print CPU and memory profile of the DDlog program                      |
| `profile cpu "on"/"off"`       |                                                  | controls the recording of differential operator runtimes; set to "on" to enable the construction of the programs CPU profile (default: "off") |
| `mssleep <ms>;`                | `mssleep 2000;`                                  | sleep for `ms` milliseconds                                            |
| `exit;`                        |                                                  | terminates execution                                                   |
| `#`                            | `# comment ending at the end of line`            | comment that ends at the end of the line                               |

**Note**: Placing a semicolon after an `insert` or `delete` operation tells DDlog to apply the
update instantly, without waiting for subsequent updates.  Comma-separated updates are only applied
once the full list of updates has been read (end of list is indicated by a semicolon).  This is
significantly more efficient and should always be the preferred option when the client wants to apply
multiple updates that are known at the same time.

## Name scoping

Relations and constructors declared in the top-level module are referred by their local names;
all other relations and constructors must be referred by their fully qualified DDlog names,
including module name:

```
// `Foo` is declared in the top-level module.
insert Foo(1),

// `Bar` is declared in a module called `modname`.
insert modname::Bar(1),
```

This is also the case for types declared inside the standard library:

```
// `Option` type is declared in `ddlog_std`.
insert R(.field1 = ddlog_std::Some{1}),
```

## `modify` command

The `modify` command

```
modify <relation> <key> <- <record>
```

modifies the value associated with primary key `<key>` in `<relation>`.  It is
only applicable to input relations declared with the `primary key` attribute.
If the specified key does not exist in the relation, the `modify` command fails.

The `<record>` argument describes how the value associated with the key must
change.  Each field in the record represents an **update** to the corresponding
field of the original record.  Think of it as a **delta** to be applied to the
existing value.  If you want to overwrite existing value with a new value,
rather than modify it, use the `insert_or_update` command instead.

The delta only needs to contain fields to be modified:

```
# Modify field `attr1` only.
modify MyRelation 1 <- MyRelation{.attr1 = "foo"}

# Modify `attr2`, only changing one of its sub-fields, called `name`.
modify MyRelation 2 <- MyRelation{.attr2 = MyStruct{.name = "bar"}}
```

Primitive types (strings, Booleans, and numeric types), as well as vectors,
are simply overwritten by `modify`.  Sets and maps have more subtle update
semantics, where the new value of the set or map is a function of both the old
value and the delta.  Sets are updated according to the following rules:

* Values present in the original set and the delta set are deleted from the
  original set.

* Values present in the delta set, but not the original set, are added to the
  original set.

* Values in the original set, but not in the delta set remain unmodified.

Example:

```
# Previous value of `attr3` is `[1, 2, 3, 4]`.  The new value after
# this command is `[1, 2, 5, 6]`.
modify MyRelation 2 <- MyRelation{.attr3 = [3, 4, 5, 6]}
```

Maps are updated according to the following rules:

* Keys present in the original map and the delta map with the same value are deleted
  from the original set.

* Keys present in the original map and the delta with different values are
  overwritten with the new value.

* Keys present in the delta, but not the original map are inserted to the
  original map.

* Keys present in the original set, but not in the delta remain unmodified.

Example:

```
# Previous value of `attr4` is `[1 -> "a", 2 -> "b", 3 -> "c", 4 -> "d"]`.  The new value after
# this command is `[1 -> "a", 2 -> "b", 4 -> "d", 5 -> "e", 6 -> "f"]`.
modify MyRelation 2 <- MyRelation{.attr3 = [(3, "c"), (4, "dd"), (5, "e"), (6, "f")]}
```

Extern types implement their own custom update semantics.  The programmer defines
this semantics by implementing the `Mutator` trait declared in
[`record.rs`](../../rust/template/differential_datalog/src/record/mod.rs).  See
[`tinyset.rs`](../../lib/tinyset.rs) for an example implementation of `trait Mutator` for
`Set64<>`.  For many types, the trait can be auto-derived using procedural macros in
[`mutator.rs`](../../rust/template/ddlog_derive/src/mutator.rs).

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
its field `x` from a JSON string stored in file `filename.json`

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
