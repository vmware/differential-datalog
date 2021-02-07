use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode,
};
use ddlog_benches::live_journal;
use std::ops::RangeInclusive;

/// Benchmark all targets using 1, 2, 3, and 4 threads
const DDLOG_WORKERS: RangeInclusive<usize> = 1..=4;

const MACRO_SAMPLES: Option<usize> = Some(10_000_000);

// Step by 50k records from 50k to 200k records
fn record_counts() -> impl Iterator<Item = usize> {
    (50_000..=200_000).step_by(50_000)
}

fn live_journal_micro(c: &mut Criterion) {
    let mut group = c.benchmark_group("LiveJournal-micro");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    for record_count in record_counts() {
        let dataset = live_journal::dataset(Some(record_count));

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
                        || (live_journal::init(thread_count), dataset.to_owned()),
                        |(ddlog, dataset)| live_journal::run(black_box(ddlog), black_box(dataset)),
                        BatchSize::PerIteration,
                    )
                },
            );
        }
    }
}

fn live_journal_macro(c: &mut Criterion) {
    let mut group = c.benchmark_group("LiveJournal-macro");
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);

    let dataset = live_journal::dataset(MACRO_SAMPLES);
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
                    || (live_journal::init(thread_count), dataset.clone()),
                    |(ddlog, data)| live_journal::run(ddlog, data),
                    BatchSize::PerIteration,
                )
            },
        );
    }
}

criterion_group!(benches, live_journal_micro, live_journal_macro);
criterion_main!(benches);
