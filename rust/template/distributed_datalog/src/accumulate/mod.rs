//! Accumulators are D3log components that participate in failure recovery
//! by tracking the contents of collections they are subscribed to and
//! dumping this contents to each new or recovered subscriber connected
//! as an output observer to the accumulator.

mod accumulator;
mod observer;
#[cfg(any(test, feature = "test"))]
mod test;
mod txndistributor;

pub use accumulator::Accumulator;
pub use accumulator::DistributingAccumulator;
pub use observer::AccumulatingObserver;
pub use txndistributor::TxnDistributor;

#[cfg(any(test, feature = "test"))]
pub use test::eq_updates;
#[cfg(any(test, feature = "test"))]
pub use test::UpdatesMockObserver;
