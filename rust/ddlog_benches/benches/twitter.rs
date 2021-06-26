use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode,
};
use ddlog_benches::twitter;
use std::ops::RangeInclusive;

/// Benchmark all targets using 1, 2, 3, and 4 threads
const DDLOG_WORKERS: RangeInclusive<usize> = 1..=4;

const MACRO_SAMPLES: Option<usize> = Some(10_000_000);

// Step by 50k records from 50k to 200k records
fn record_counts() -> impl Iterator<Item = usize> {
    (50_000..=200_000).step_by(50_000)
}

fn twitter_micro(c: &mut Criterion) {
    let mut group = c.benchmark_group("twitter-micro");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    for record_count in record_counts() {
        let dataset = twitter::dataset(Some(record_count));

        for thread_count in DDLOG_WORKERS.rev() {
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
                        || (ddlog_benches::init(thread_count), dataset.to_owned()),
                        |(ddlog, dataset)| ddlog_benches::run(black_box(ddlog), black_box(dataset)),
                        BatchSize::PerIteration,
                    )
                },
            );
        }
    }
}

fn twitter_macro(c: &mut Criterion) {
    let mut group = c.benchmark_group("twitter-macro");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    let dataset = twitter::dataset(MACRO_SAMPLES);
    for thread_count in DDLOG_WORKERS.rev() {
        group.bench_with_input(
            BenchmarkId::new(
                format!(
                    "{} thread{}",
                    thread_count,
                    if thread_count == 1 { "" } else { "s" },
                ),
                format!("{} samples", dataset.len()),
            ),
            &dataset,
            |b, dataset| {
                b.iter_batched(
                    || (ddlog_benches::init(thread_count), dataset.clone()),
                    |(ddlog, data)| ddlog_benches::run(ddlog, data),
                    BatchSize::PerIteration,
                )
            },
        );
    }
}

criterion_group!(benches, twitter_micro, twitter_macro);
criterion_main!(benches);
