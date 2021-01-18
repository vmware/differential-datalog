use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use ddlog_benches::citations;
use std::{ops::RangeInclusive, time::Duration};

/// Benchmark all targets using 1, 2, 3, and 4 threads
const DDLOG_WORKERS: RangeInclusive<usize> = 1..=4;

// Increment the number of records taken by 100,000 until we're taking 1 million records
fn record_counts() -> impl Iterator<Item = usize> {
    // TODO: I'd rather go to higher increments, but it's *way* to slow.
    //       Try 200000 if you feel brave, they take an hour each to run
    //       the benches through, even at a sample size of 10
    (100_000..=100_000).step_by(100_000)
}

fn citations(c: &mut Criterion) {
    let mut group = c.benchmark_group("citations");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(10));

    for record_count in record_counts() {
        let dataset = citations::dataset(record_count);

        for thread_count in DDLOG_WORKERS {
            group.bench_with_input(
                BenchmarkId::new(
                    format!(
                        "{} thread{}",
                        thread_count,
                        if thread_count == 1 { "" } else { "s" },
                    ),
                    format!("{} records", record_count),
                ),
                &dataset,
                |b, dataset| {
                    b.iter_batched(
                        || (citations::init(thread_count), dataset.to_owned()),
                        |(ddlog, dataset)| citations::run(black_box(ddlog), black_box(dataset)),
                        BatchSize::PerIteration,
                    )
                },
            );
        }
    }
}

fn citations_throughput(c: &mut Criterion) {
    for record_count in record_counts() {
        let mut group = c.benchmark_group("citations");
        group.sample_size(10);
        group.warm_up_time(Duration::from_secs(10));
        group.throughput(Throughput::Elements(record_count as _));

        let dataset = citations::dataset(record_count);

        for thread_count in DDLOG_WORKERS {
            group.bench_with_input(
                BenchmarkId::new(
                    format!(
                        "{} thread{}",
                        thread_count,
                        if thread_count == 1 { "" } else { "s" },
                    ),
                    format!("{} records", record_count),
                ),
                &dataset,
                |b, dataset| {
                    b.iter_batched(
                        || (citations::init(thread_count), dataset.to_owned()),
                        |(ddlog, dataset)| citations::run(black_box(ddlog), black_box(dataset)),
                        BatchSize::PerIteration,
                    )
                },
            );
        }
    }
}

criterion_group!(benches, citations, citations_throughput);
criterion_main!(benches);
