use std::time::Duration;

use backon::ExponentialBuilder;

#[derive(Clone, Copy)]
pub struct RetryPolicy {
    pub max_times: usize,
    pub min_delay: Duration,
    pub max_delay: Duration,
}

impl RetryPolicy {
    pub fn backoff(self) -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_jitter()
            .with_max_times(self.max_times)
            .with_min_delay(self.min_delay)
            .with_max_delay(self.max_delay)
    }
}
