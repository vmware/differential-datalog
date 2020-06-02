# DBpedia Ownership Example
This example computes the recursive ownership relation of entities according
to a data set of [10,000 pairs of entities from DBpedia](./data/dbpedia_ownership.csv)
that are related to each other with the [`owner` property](http://dbpedia.org/ontology/owner).

The configuration uses D3log for instantiation, with a single source file containing 
transactions (default: `data/subsidiaries.ddlog.dat`) and an output file for the 
computed ownership relation (default: `ownership.dump`).

## Usage
On first use, the [DDlog source code](../dbpedia_ownership.dl) must be compiled using 
```shell script
ddlog -i dbpedia_ownership.dl -L ../../lib
```
in the parent directory.

Use the provided Makefile in this directory for program execution. `make run` generates 
a transaction file containing all 10,000 entity pairs, where each transaction
contains 100 insertions and initiates the D3log computation with it.
At the end of the transactions an `exit;` command is issued that is used for timing the 
performance of the computation.

### Benchmark for Accumulator
The goal of this example is to provide a simple benchmark for the `DistributingAccumulator` 
component. Given the [provided patch](remove-accumulator-in-instantiate.patch), one can compare
the runtimes of a simple D3log configuration with or without the use of an accumulator.
In the root directory of the Differential Datalog repository, execute the following command
to remove the accumulator from the D3log configuration:
```shell script
git apply test/datalog_tests/dbpedia_ownership/remove-accumulator-in-instantiate.patch
```
Use the `-R` option from `git apply` to add the accumulator back to the configuration.