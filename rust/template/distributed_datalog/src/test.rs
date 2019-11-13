use std::panic::catch_unwind;
use std::panic::AssertUnwindSafe;
use std::panic::UnwindSafe;
use std::time::Duration;

use waitfor::wait_for;

fn await_expected_impl<F>(timeout: Duration, interval: Duration, mut op: F)
where
    F: FnMut() + UnwindSafe,
{
    let op = || -> Result<Option<()>, ()> {
        // All we care about for the sake of testing here are assertion
        // failures. So if we encounter one (in the form of a panic) we
        // map that to a retry.
        match catch_unwind(AssertUnwindSafe(&mut op)) {
            Ok(_) => Ok(Some(())),
            Err(_) => Ok(None),
        }
    };

    let result = wait_for(timeout, interval, op);
    match result {
        Ok(Some(_)) => (),
        Ok(None) => panic!("time out waiting for expected result"),
        Err(e) => panic!("failed to await expected result: {:?}", e),
    }
}

/// Await some expected result.
///
/// This function is meant to be used for testing only. The intention is
/// to put some assertions that should eventually hold in there and if
/// no panics are detected the result is a success.
/// The function times out if an expected result hasn't been reached
/// after 5s, at which point a panic will occur, indicating the timeout
/// condition.
pub fn await_expected<F>(op: F)
where
    F: FnMut() + UnwindSafe,
{
    await_expected_impl(Duration::from_secs(5), Duration::from_millis(1), op)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn immediate_success() {
        await_expected(|| assert!(true));
    }

    #[test]
    fn eventual_success() {
        static mut COUNT: u32 = 0;

        await_expected(|| unsafe {
            COUNT += 1;
            assert_eq!(COUNT, 5);
        });

        unsafe { assert_eq!(COUNT, 5) };
    }

    #[test]
    #[should_panic(expected = "time out waiting for expected result")]
    fn timeout() {
        await_expected_impl(Duration::from_millis(1), Duration::from_nanos(1), || {
            assert!(false, "induced failure")
        });
    }
}
