Each DDlog program generates a rust package during compilation. The package
contains an API to the running DDlog instance, providing methods in [`ddlog.h`](ddlog.h).
Following are the steps to interact with a running DDlog program from Rust
through the API:

1. Compile the DDlog program to produce the rust code.
2. In the client package, import the root of the generated package as well as
the `differential_datalog` directory in it. This links the client code to the
DDlog program.
3. Write code against the API documented in [`ddlog.h`](ddlog.h). The rust
   implementation for the API can be found in [`api.rs`](api.rs),
   [`differential_datalog/record.rs`](differential_datalog/record.rs) etc.

An example client can be found in
[`test/datalog_tests/api_test`](../../test/datalog_tests/api_test).
