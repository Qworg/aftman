//! A bounded, head-and-tail preserving log of worker output.
//!
//! A worker's output is unbounded and mostly noise, but two slices of it matter:
//! the **head**, which shows what the agent was asked to do and what it did first,
//! and the **tail**, which shows where it ended up. The middle is discarded.
//!
//! This is a hard bound on daemon memory: a worker that prints ten gigabytes must
//! not be able to grow the supervisor's footprint. Everything here is byte-capped
//! and truncates on UTF-8 character boundaries, so a truncated log is always valid
//! text.

use serde::{Deserialize, Serialize};

/// Which pipe a chunk arrived on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stream {
    Stdout,
    Stderr,
}

impl Stream {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

/// Truncate `text` to at most `max` **bytes**, on a character boundary.
fn truncate_bytes(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Keep at most the last `max` **bytes** of `text`, on a character boundary.
fn keep_last_bytes(text: &str, max: usize) -> &str {
    if text.len() <= max {
        return text;
    }
    let mut start = text.len() - max;
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

/// Bounded worker output: a capped head, a capped tail, and counters for what was
/// dropped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundedLog {
    head: String,
    tail: String,
    head_capacity: usize,
    tail_capacity: usize,
    /// Every byte ever pushed, including dropped ones.
    total_bytes: u64,
    /// Bytes that arrived since the last [`BoundedLog::take_unflushed`].
    unflushed_bytes: u64,
}

impl BoundedLog {
    /// `head_capacity` and `tail_capacity` are byte caps, matching the
    /// `obs.tail_chars` default of 12 000 per captured tail.
    #[must_use]
    pub fn new(head_capacity: usize, tail_capacity: usize) -> Self {
        Self {
            head: String::new(),
            tail: String::new(),
            head_capacity,
            tail_capacity,
            total_bytes: 0,
            unflushed_bytes: 0,
        }
    }

    /// Append a chunk.
    ///
    /// The head fills once and then never grows. The tail keeps only the most
    /// recent `tail_capacity` bytes.
    pub fn push(&mut self, _stream: Stream, text: &str) {
        if text.is_empty() {
            return;
        }
        self.total_bytes = self.total_bytes.saturating_add(text.len() as u64);
        self.unflushed_bytes = self.unflushed_bytes.saturating_add(text.len() as u64);

        if self.head.len() < self.head_capacity {
            let room = self.head_capacity - self.head.len();
            self.head.push_str(truncate_bytes(text, room));
        }

        self.tail.push_str(text);
        if self.tail.len() > self.tail_capacity {
            let trimmed = keep_last_bytes(&self.tail, self.tail_capacity).to_owned();
            self.tail = trimmed;
        }
    }

    /// The head, truncated to at most `max` bytes.
    #[must_use]
    pub fn head(&self, max: usize) -> &str {
        truncate_bytes(&self.head, max)
    }

    /// The tail, truncated to at most `max` bytes.
    #[must_use]
    pub fn tail(&self, max: usize) -> &str {
        keep_last_bytes(&self.tail, max)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.total_bytes == 0
    }

    /// Every byte ever pushed.
    #[must_use]
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Bytes ever pushed minus bytes currently retained in head and tail.
    ///
    /// Non-zero means the log is lossy, which the observation reports so that a
    /// model is never shown a silently clipped tail as though it were complete.
    #[must_use]
    pub fn dropped_bytes(&self) -> u64 {
        let retained = self.head.len() as u64 + self.tail.len() as u64;
        self.total_bytes.saturating_sub(retained)
    }

    /// Bytes received since the last call, resetting the counter.
    ///
    /// The runtime uses this to enforce "flush `WorkerOutput` only when new bytes
    /// actually arrived", so an idle tick never emits an empty output event.
    pub fn take_unflushed(&mut self) -> u64 {
        std::mem::take(&mut self.unflushed_bytes)
    }

    #[must_use]
    pub const fn has_unflushed(&self) -> bool {
        self.unflushed_bytes > 0
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedLog, Stream};

    #[test]
    fn head_fills_once_and_tail_keeps_the_latest() {
        let mut log = BoundedLog::new(10, 10);
        log.push(Stream::Stdout, "abcdefghij");
        log.push(Stream::Stdout, "KLMNOPQRST");
        assert_eq!(log.head(100), "abcdefghij");
        assert_eq!(log.tail(100), "KLMNOPQRST");
        assert_eq!(log.total_bytes(), 20);
    }

    #[test]
    fn memory_is_bounded_regardless_of_input_size() {
        let mut log = BoundedLog::new(64, 64);
        for _ in 0..10_000 {
            log.push(Stream::Stdout, "a very chatty worker says hello\n");
        }
        assert!(log.head(1_000_000).len() <= 64);
        assert!(log.tail(1_000_000).len() <= 64);
        assert!(log.total_bytes() > 300_000);
        assert!(log.dropped_bytes() > 0);
    }

    #[test]
    fn truncation_never_splits_a_character() {
        // 'é' is two bytes; a naive byte slice at an odd offset would panic.
        let mut log = BoundedLog::new(5, 5);
        log.push(Stream::Stderr, "éééééééé");
        let head = log.head(5);
        let tail = log.tail(5);
        assert!(head.len() <= 5);
        assert!(tail.len() <= 5);
        // Still valid UTF-8 by construction: these are &str.
        assert!(head.chars().all(|c| c == 'é'));
        assert!(tail.chars().all(|c| c == 'é'));
    }

    #[test]
    fn unflushed_bytes_track_new_content() {
        let mut log = BoundedLog::new(8, 8);
        assert!(!log.has_unflushed());
        log.push(Stream::Stdout, "hi");
        assert!(log.has_unflushed());
        assert_eq!(log.take_unflushed(), 2);
        assert!(!log.has_unflushed());
        // An idle tick pushes nothing, so nothing is unflushed.
        assert_eq!(log.take_unflushed(), 0);
    }

    #[test]
    fn empty_pushes_do_not_count_as_content() {
        let mut log = BoundedLog::new(8, 8);
        log.push(Stream::Stdout, "");
        assert!(log.is_empty());
        assert!(!log.has_unflushed());
    }
}
