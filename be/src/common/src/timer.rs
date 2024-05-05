use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

pub struct TimerLoggerGuard {
    start_time: Instant,
    msg: String,
}

impl TimerLoggerGuard {
    pub fn new(msg: String) -> TimerLoggerGuard {
        TimerLoggerGuard {
            start_time: Instant::now(),
            msg,
        }
    }

    fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

impl Drop for TimerLoggerGuard {
    fn drop(&mut self) {
        let elapsed = self.elapsed();
        info!("{}, During time: {}ms", self.msg, elapsed.as_millis());
    }
}
#[macro_export]
macro_rules! during_time_info {
    ($msg:expr) => {
        let _guard = TimerLoggerGuard::new($msg.into());
    };
}