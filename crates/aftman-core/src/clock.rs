//! The injected clock.
//!
//! The domain never reads the system clock directly. Two distinct notions of time
//! are needed and they are deliberately separate:
//!
//! - **Wall clock** ([`Clock::now`]) — for human-readable timestamps on events and
//!   run headers. It can jump backwards; nothing orders off it.
//! - **Monotonic instant** ([`Clock::instant`]) — for measuring elapsed time,
//!   timeouts, and debounce windows. It never jumps.
//!
//! Injecting both is what lets a test drive a 30-second steering grace window or a
//! two-hour overall timeout without waiting.

use std::sync::{Arc, Mutex};

use time::OffsetDateTime;
use tokio::time::Instant;

/// Source of wall-clock and monotonic time.
pub trait Clock: Send + Sync + 'static {
    /// Current UTC wall-clock time. For timestamps only.
    fn now(&self) -> OffsetDateTime;

    /// Monotonic instant. For elapsed measurement, timeouts, and debounce.
    fn instant(&self) -> Instant;
}

/// The real clock. Used by the binary and by any test that does not need
/// determinism. Pair with `tokio::time::pause()` when a test needs to advance time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }

    fn instant(&self) -> Instant {
        Instant::now()
    }
}

/// A clock that never moves unless it is told to.
///
/// Used for byte-exact snapshot tests: a timestamp that changes between two runs
/// makes every serialized comparison meaningless, so tests freeze it.
#[derive(Debug, Clone)]
pub struct FixedClock {
    wall: Arc<Mutex<OffsetDateTime>>,
    mono: Instant,
}

impl FixedClock {
    #[must_use]
    pub fn new(wall: OffsetDateTime) -> Self {
        Self {
            wall: Arc::new(Mutex::new(wall)),
            mono: Instant::now(),
        }
    }

    /// A frozen clock at the Unix epoch. Timestamps are stable and obviously fake.
    #[must_use]
    pub fn epoch() -> Self {
        Self::new(OffsetDateTime::UNIX_EPOCH)
    }

    /// Move the wall clock. Monotonic time is unaffected, because a test that
    /// jumps the wall clock forwards must not thereby make a timeout fire.
    pub fn set_wall(&self, wall: OffsetDateTime) {
        *self.lock() = wall;
    }

    /// Recover rather than panic on a poisoned lock.
    ///
    /// The guarded value is a `Copy` timestamp with no cross-field invariant, so a
    /// panic elsewhere while holding the lock cannot leave it inconsistent.
    /// Poisoning therefore carries no information worth aborting over.
    fn lock(&self) -> std::sync::MutexGuard<'_, OffsetDateTime> {
        self.wall
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        *self.lock()
    }

    fn instant(&self) -> Instant {
        self.mono
    }
}

#[cfg(test)]
mod tests {
    use super::{Clock, FixedClock};
    use time::OffsetDateTime;
    use time::macros::datetime;

    #[test]
    fn fixed_clock_is_stable_until_moved() {
        let clock = FixedClock::new(datetime!(2026-01-02 03:04:05 UTC));
        assert_eq!(clock.now(), datetime!(2026-01-02 03:04:05 UTC));
        assert_eq!(clock.now(), clock.now());

        clock.set_wall(datetime!(2026-06-07 08:09:10 UTC));
        assert_eq!(clock.now(), datetime!(2026-06-07 08:09:10 UTC));
    }

    #[test]
    fn moving_the_wall_clock_does_not_move_the_monotonic_one() {
        let clock = FixedClock::new(OffsetDateTime::UNIX_EPOCH);
        let before = clock.instant();
        // Jump the wall clock a day forward. A timeout measured off `instant()`
        // must not observe any elapsed time at all.
        clock.set_wall(OffsetDateTime::UNIX_EPOCH + time::Duration::days(1));
        assert_eq!(clock.instant(), before);
    }
}
