## Timely Profiling

Here we describe how to profile timely events and use the accompanying script to compute information about
your ddlog program's execution.

### Turning on timely profiling
Given a ddlog program, you can turn on timely profiling by adding `profile timely on;` to your ddlog program's
command file. Putting it at the beggining will profile the entire program. For example for souffle0:

```bash
> pwd
/home/omar/differential-datalog/test/souffle0
> more souffle.dat
profile timely on;
start;
insert RDirectSuperclass_shadow("syntaxtree.IntegerType","java.lang.Object"),
insert RDirectSuperclass_shadow("sun.security.util.PropertyExpander","java.lang.Object"),
...
```

Also include `profile;` before the `exit` command.
```bash
> tail -n 3 souffle.dat
dump RReachable;
profile;
exit;
```

### Executing DDlog Program
Now you can execute your program. Timely events will be emitted via a `stats.csv` in your current working
directory. Running the command:
```bash
> ./souffle_ddlog/target/release/souffle_cli -w8 < souffle.dat > souffle.dump
```
will generate the `stats.csv` file.

You can run this file for different number of workers to understand how different metrics of your program
scale along with number of workers. Namely, operator execution time, operator number of invocations,
operator average time spent per invocation, progress events per operator, and data message sizes.

We can now move the generated CSV to a separate directory to avoid subsequent runs from overwriting their
results.
```bash
> mkdir timely_events/
> mv stats.csv timely_events/stats_w8.csv
```

### Getting CSV Directory in Correct Format
We run the procedure above for different number of workers: 1 worker, 2 workers, 4 workers, and 8 workers.

To process these files we must first get them in the correct format. The computing script `generate_stats.py`
expects a single directory, containing all the CSV files for a program's execution and *only* those files.
Extraneous files in that directory will lead to an error.

The files are expected to be named in the format of: `filename_wN.csv` where `filename` can be any string not
containing underscores. The `N` must be an integer representating the number of workers the program that
generated this file used.

Example directory:
```
> ls timely_events/
stats_w1.csv  stats_w2.csv  stats_w4.csv  stats_w8.csv
```

The computing script will automatically infer the number of workers from the file names.

### Executing computing script.
The `generate_stats.py` script takes the general form:
```
Usage: python generate_stats.py path/to/csv_file_dir/ command output_file_name.csv
```
Where `path/to/csv_file_dir/` is a directory as described above and `command` is one of the following four
commands:
- **exe_invocation_table**: Generate a table containing per-operator execution times, total invocation times.
  along with normalized values over one worker. This is useful to see how different operators scale as more
  workers are added.
- **exe_time_summary**: Output to stdout some quick numbers to see how different parts of the program
  scale as workers are added. Example:
  ```
  > python3 generate_stats.py /home/omar/differential-datalog/test/souffle0/timely_events/ exe_time_summary ~/output.csv
  W1 dataflow operator execution: 10.924s
  W1 Non-nested operators execution: 10.640s
  W1 Guarded seconds: 2.961s

  W2 dataflow operator execution: 14.178s
  W2 Non-nested operators execution: 13.485s
  W2 Guarded seconds: 4.821s
  
  W4 dataflow operator execution: 28.113s
  W4 Non-nested operators execution: 26.399s
  W4 Guarded seconds: 15.650s
  
  W8 dataflow operator execution: 49.248s
  W8 Non-nested operators execution: 44.908s
  W8 Guarded seconds: 30.605s
  ```
- **progress_table**: Generate a table containing number of progress events per operator by number
  of workers.

| operator_id | progress_events_1w | progress_events_2w | progress_events_4w | progress_events_8w | operator_name | operator_address |
| --- | --- | --- | --- | --- | --- | --- |
| 2.0         | 2                  | 4                  | 8                  | 16                 | [Concatenate] | [0-2-]           |
| 5.0         | 2                  | 2                  | 2                  | 2                  | [FlatMap]     | [0-4-]           |
| 7.0         | 3                  | 6                  | 12                 | 24                 | [Concatenate] | [0-5-]           |
| 10.0        | 2                  | 2                  | 2                  | 2                  | [FlatMap]     | [0-6-]           |
| 12.0        | 2                  | 4                  | 8                  | 16                 | [Arrange]     | [0-7-]           |

- **data_sizes_table**: Generate a table describing mean and median message size for message data per channel (timely reports this information per channel not per operator). With additional work we could map this data back to the operators.
