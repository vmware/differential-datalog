Each DDlog program generates a Rust crate during compilation. The crate,
generated from the template in this directory, contains the DDlog engine
and a runtime API to the DDlog instance for multiple languages:
- Rust in [`src/api.rs`](src/api.rs)
- C in [`ddlog.h`](ddlog.h)
- Java (TODO)

Following are the steps to run and interact with the DDlog program from Rust
through the API:

1. Compile the DDlog program to produce Rust code:
  ```
  ddlog -i <prog>.dl -L<path_to_lib_directory>
  ```
  This generates a directory called `<prog>_ddlog` containing the Rust crate.

2. In the client crate's `Cargo.toml`, import the root of the generated package as well as
the `differential_datalog` directory in it. This links the client code to the
DDlog program (see example in
[`test/datalog_tests/api/Cargo.toml`](../../test/datalog_tests/api/Cargo.toml)).

3. Write code against the API documented in [`ddlog.h`](ddlog.h). The Rust
   implementation for the API can be found in [`src/api.rs`](src/api.rs),
   [`differential_datalog/record.rs`](differential_datalog/record.rs) etc.

An example client can be found in
[`test/datalog_tests/api`](../../test/datalog_tests/api).
