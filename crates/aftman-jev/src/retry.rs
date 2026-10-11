//! Retry policy and `Retry-After` handling.
//!
//! Timing is injected rather than read from the clock, so a test can assert that a
//! `429` with `retry-after-ms: 100` is honoured without waiting 100 milliseconds.
//! See `docs/03-jev.md` §4.

use std::time::Duration;

use time::OffsetDateTime;

/// How many times to retry, and how long to wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_retries: u32,
    /// First backoff step. Subsequent steps double.
    pub base_delay: Duration,
    pub max_delay: Duration,
    /// Add ±30% jitter, so a fleet of clients does not synchronise its retries.
    pub jitter: bool,
    /// A `Retry-After` longer than this aborts instead of sleeping. A supervisor
    /// that blocks for minutes is worse than one that reports uncertainty.
    pub max_retry_after: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 2,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(2),
            jitter: true,
            max_retry_after: Duration::from_secs(30),
        }
    }
}

impl RetryPolicy {
    /// No retries and no delay: for tests and for `doctor --probe`, where a hang is
    /// worse than a failure.
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: false,
            max_retry_after: Duration::ZERO,
        }
    }

    /// The backoff before attempt `attempt` (0-based: the delay after the first
    /// failure is `attempt = 0`).
    ///
    /// `seed` drives the jitter. It is a parameter rather than a clock read so the
    /// sequence is reproducible in a test.
    #[must_use]
    pub fn backoff(&self, attempt: u32, seed: u64) -> Duration {
        let factor = 1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX);
        let base = self.base_delay.saturating_mul(factor);
        let capped = base.min(self.max_delay);
        if !self.jitter {
            return capped;
        }
        // ±30%: 0.7..=1.3 of the capped delay.
        let millis = capped.as_millis() as u64;
        let spread = millis * 6 / 10;
        let offset = splitmix64(seed.wrapping_add(u64::from(attempt))) % (spread + 1);
        Duration::from_millis((millis - spread / 2).saturating_add(offset))
    }
}

/// A small deterministic mixer, so jitter is reproducible for a given seed.
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A `Retry-After`, as the provider asked for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryAfter {
    /// The provider named a delay, and it is within our ceiling.
    Wait(Duration),
    /// The provider named a delay longer than we are willing to block for.
    TooLong(Duration),
    /// The header was present but unparseable, or absent.
    Unspecified,
}

/// Read `retry-after-ms` and `retry-after` from a response.
///
/// `retry-after-ms` wins: it is the more precise header, and a provider that sends
/// both means the millisecond value.
#[must_use]
pub fn retry_after(headers: &[(String, String)], now: OffsetDateTime, max: Duration) -> RetryAfter {
    let lookup = |name: &str| {
        headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim().to_owned())
    };

    if let Some(raw) = lookup("retry-after-ms") {
        if let Some(delay) = parse_millis(&raw) {
            return classify(delay, max);
        }
    }
    if let Some(raw) = lookup("retry-after") {
        if let Some(delay) = parse_seconds(&raw) {
            return classify(delay, max);
        }
        if let Some(delay) = parse_http_date(&raw, now) {
            return classify(delay, max);
        }
    }
    RetryAfter::Unspecified
}

fn classify(delay: Duration, max: Duration) -> RetryAfter {
    if delay > max {
        RetryAfter::TooLong(delay)
    } else {
        RetryAfter::Wait(delay)
    }
}

fn parse_millis(raw: &str) -> Option<Duration> {
    let value: f64 = raw.parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    Some(Duration::from_millis(value as u64))
}

fn parse_seconds(raw: &str) -> Option<Duration> {
    let value: f64 = raw.parse().ok()?;
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    Some(Duration::from_millis((value * 1000.0) as u64))
}

fn parse_http_date(raw: &str, now: OffsetDateTime) -> Option<Duration> {
    // HTTP-date is IMF-fixdate, which RFC 2822 parses for the shapes providers
    // actually send.
    use time::format_description::well_known::Rfc2822;
    let parsed = OffsetDateTime::parse(raw, &Rfc2822).ok()?;
    let delta = parsed - now;
    let millis = delta.whole_milliseconds();
    if millis <= 0 {
        return Some(Duration::ZERO);
    }
    Some(Duration::from_millis(millis as u64))
}

#[cfg(test)]
mod tests {
    use super::{RetryAfter, RetryPolicy, retry_after};
    use std::time::Duration;
    use time::macros::datetime;

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn retry_after_ms_is_preferred_over_retry_after() {
        let value = retry_after(
            &headers(&[("retry-after-ms", "100"), ("retry-after", "5")]),
            datetime!(2026-01-01 00:00:00 UTC),
            Duration::from_secs(30),
        );
        assert_eq!(value, RetryAfter::Wait(Duration::from_millis(100)));
    }

    #[test]
    fn retry_after_seconds_parse() {
        let value = retry_after(
            &headers(&[("Retry-After", "2")]),
            datetime!(2026-01-01 00:00:00 UTC),
            Duration::from_secs(30),
        );
        // Header lookup is case-insensitive.
        assert_eq!(value, RetryAfter::Wait(Duration::from_secs(2)));
    }

    #[test]
    fn an_http_date_is_converted_to_a_delay() {
        let value = retry_after(
            &headers(&[("retry-after", "Thu, 01 Jan 2026 00:00:05 GMT")]),
            datetime!(2026-01-01 00:00:00 UTC),
            Duration::from_secs(30),
        );
        assert_eq!(value, RetryAfter::Wait(Duration::from_secs(5)));
    }

    #[test]
    fn a_date_in_the_past_means_no_wait() {
        let value = retry_after(
            &headers(&[("retry-after", "Thu, 01 Jan 2020 00:00:00 GMT")]),
            datetime!(2026-01-01 00:00:00 UTC),
            Duration::from_secs(30),
        );
        assert_eq!(value, RetryAfter::Wait(Duration::ZERO));
    }

    #[test]
    fn a_longer_wait_than_we_allow_is_reported_rather_than_slept_through() {
        // Blocking a supervisor for minutes is worse than reporting uncertainty.
        let value = retry_after(
            &headers(&[("retry-after", "600")]),
            datetime!(2026-01-01 00:00:00 UTC),
            Duration::from_secs(30),
        );
        assert_eq!(value, RetryAfter::TooLong(Duration::from_secs(600)));
    }

    #[test]
    fn missing_and_malformed_headers_are_unspecified() {
        let now = datetime!(2026-01-01 00:00:00 UTC);
        assert_eq!(
            retry_after(&[], now, Duration::from_secs(30)),
            RetryAfter::Unspecified
        );
        assert_eq!(
            retry_after(
                &headers(&[("retry-after", "soon")]),
                now,
                Duration::from_secs(30)
            ),
            RetryAfter::Unspecified
        );
        assert_eq!(
            retry_after(
                &headers(&[("retry-after", "-5")]),
                now,
                Duration::from_secs(30)
            ),
            RetryAfter::Unspecified
        );
    }

    #[test]
    fn backoff_grows_and_is_capped() {
        let policy = RetryPolicy {
            jitter: false,
            ..RetryPolicy::default()
        };
        assert_eq!(policy.backoff(0, 0), Duration::from_millis(250));
        assert_eq!(policy.backoff(1, 0), Duration::from_millis(500));
        assert_eq!(policy.backoff(2, 0), Duration::from_secs(1));
        assert_eq!(policy.backoff(9, 0), Duration::from_secs(2), "capped");
    }

    #[test]
    fn jitter_stays_within_thirty_percent_and_is_reproducible() {
        let policy = RetryPolicy {
            jitter: true,
            ..RetryPolicy::default()
        };
        for seed in 0..64u64 {
            let delay = policy.backoff(0, seed).as_millis() as u64;
            assert!(
                (175..=325).contains(&delay),
                "seed {seed} produced {delay}ms, outside ±30% of 250ms"
            );
        }
        assert_eq!(
            policy.backoff(0, 7),
            policy.backoff(0, 7),
            "the same seed must produce the same delay, so tests are deterministic"
        );
    }

    #[test]
    fn no_retry_policy_never_waits() {
        let policy = RetryPolicy::none();
        assert_eq!(policy.max_retries, 0);
        assert_eq!(policy.backoff(0, 0), Duration::ZERO);
        assert_eq!(policy.backoff(3, 9), Duration::ZERO);
    }
}
