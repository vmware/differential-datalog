# ddlog_benches

DDlog has a unique benchmarking scenario since it's a compiler, so benchmarking the programs
it generates is a little more involved than normal, which is why we use [`cargo-make`] to automate
the setup, build and bench process in a cross-platform way. To run the benchmarks in this crate,
first install `cargo-make` with the following command (`--force` makes cargo install the latest version)

```sh
cargo install --force cargo-make
```

To run all benchmarks use the following command

```sh
<<<<<<< HEAD
cargo make benchmarks
=======
cargo make benchmark-all
>>>>>>> 7e78c21d... Added ddlog code and added docs on running benches
```

When adding a benchmark, add the required ddlog code to `/ddlog`, any datasets to `/data` (preferably in a
compressed format), add a module within `/src` containing your support code and the actual benchmark within
`/benches` and finally add a `[[bench]]` entry into the `Cargo.toml` along with any required dependencies.
For info on writing benchmarks see the [criterion user guide] and the [criterion docs].

<<<<<<< HEAD
### Other supported commands

- `cargo make bench-twitter`: Only run the Twitter benchmarks
- `cargo make bench-livejournal`: Only run the LiveJournal benchmarks
- `cargo make build-ddlog`: Only build the generated ddlog code
- `cargo make download-data`: Download the datasets required for benchmarking

=======
>>>>>>> 7e78c21d... Added ddlog code and added docs on running benches
[`cargo-make`]: https://github.com/sagiegurari/cargo-make
[criterion user guide]: https://bheisler.github.io/criterion.rs/book/index.html
[criterion docs]: https://docs.rs/criterion
