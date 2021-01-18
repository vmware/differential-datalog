use criterion::{
    measurement::{Measurement, ValueFormatter},
    Throughput,
};
use std::time::{Duration, Instant};

/// A custom criterion measurement that measures the number of records
/// ingested for a given time period
pub struct RecordsIngested;

impl Measurement for RecordsIngested {
    type Intermediate = Instant;
    type Value = Duration;

    fn start(&self) -> Self::Intermediate {
        Instant::now()
    }

    fn end(&self, start_time: Self::Intermediate) -> Self::Value {
        start_time.elapsed()
    }

    fn add(&self, &elapsed1: &Self::Value, &elapsed2: &Self::Value) -> Self::Value {
        elapsed1 + elapsed2
    }

    fn zero(&self) -> Self::Value {
        Duration::from_secs(0)
    }

    fn to_f64(&self, elapsed: &Self::Value) -> f64 {
        elapsed.as_secs_f64()
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        &RecordsIngestedFormatter
    }
}

/// A custom criterion formatter for the number of records ingested for
/// a given period of time
pub struct RecordsIngestedFormatter;

impl RecordsIngestedFormatter {
    fn records_per_second(&self, records: f64, typical: f64, values: &mut [f64]) -> &'static str {
        let records_per_second = records * (1e9 / typical);

        let (denominator, unit) = if records_per_second < 1000.0 {
            (1.0, " records/s")
        } else if records_per_second < 1000.0 * 1000.0 {
            (1000.0, "thousand records/s")
        } else if records_per_second < 1000.0 * 1000.0 * 1000.0 {
            (1000.0 * 1000.0, "million records/s")
        } else {
            (1000.0 * 1000.0 * 1000.0, "billion records/s")
        };

        for val in values {
            let records_per_second = records * (1e9 / *val);
            *val = records_per_second / denominator;
        }

        unit
    }
}

impl ValueFormatter for RecordsIngestedFormatter {
    fn scale_throughputs(
        &self,
        typical: f64,
        throughput: &Throughput,
        values: &mut [f64],
    ) -> &'static str {
        match *throughput {
            Throughput::Elements(records) => {
                self.records_per_second(records as f64, typical, values)
            }

            Throughput::Bytes(_) => {
                panic!("RecordsPerSecond can only be called with `Throughput::Elements`")
            }
        }
    }

    fn scale_values(&self, ns: f64, values: &mut [f64]) -> &'static str {
        let (factor, unit) = if ns < 10f64.powi(0) {
            (10f64.powi(3), "ps")
        } else if ns < 10f64.powi(3) {
            (10f64.powi(0), "ns")
        } else if ns < 10f64.powi(6) {
            (10f64.powi(-3), "μs")
        } else if ns < 10f64.powi(9) {
            (10f64.powi(-6), "ms")
        } else {
            (10f64.powi(-9), "s")
        };

        for val in values {
            *val *= factor;
        }

        unit
    }

    fn scale_for_machines(&self, _values: &mut [f64]) -> &'static str {
        "records/s"
    }
}
