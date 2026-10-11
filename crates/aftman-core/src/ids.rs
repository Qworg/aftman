//! Time-ordered identifiers.
//!
//! Runs, workers, and attempts use UUIDv7, which embeds a millisecond timestamp in
//! the high bits. That gives two useful properties for free: IDs sort roughly in
//! creation order when they appear in a log or a directory listing, and an ID
//! generated on one machine does not collide with one generated on another.
//!
//! Sequence numbers ([`EventSeq`], [`AssessmentSeq`]) are monotonic *per run* and
//! start at 1. They, not timestamps, define event order — a clock that jumps
//! backwards must never reorder history.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::IdError;

macro_rules! uuid_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generate a new time-ordered identifier.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            #[must_use]
            pub const fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(raw: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(raw).map(Self).map_err(|_| IdError::MalformedUuid)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

uuid_id!(
    /// Identifies one supervised run.
    RunId
);
uuid_id!(
    /// Identifies one worker (a coding agent, or a verifier) within a run.
    WorkerId
);
uuid_id!(
    /// Identifies one attempt at a job. Retries create a new attempt.
    AttemptId
);
uuid_id!(
    /// Identifies a raised escalation awaiting a human answer.
    EscalationId
);

/// Monotonic event sequence number, unique within a run, starting at 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventSeq(pub u64);

impl EventSeq {
    pub const FIRST: Self = Self(1);

    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for EventSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Monotonic assessment sequence number, unique within a run, starting at 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AssessmentSeq(pub u64);

impl AssessmentSeq {
    pub const FIRST: Self = Self(1);

    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for AssessmentSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Construct from anything string-like.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({:?})", stringify!($name), self.0)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
    };
}

string_id!(
    /// A logical area of supervision, for example `core.completion`.
    ResponsibilityId
);
string_id!(
    /// A check within a responsibility, for example `ready_to_finish`.
    CheckId
);
string_id!(
    /// An evidence provider, for example `git_diff` or `command:tests`.
    EvidenceId
);
string_id!(
    /// A rule within a responsibility, for example `core.worker-health:stuck`.
    ///
    /// Every rule has one, built-ins included, because the policy tie-break ladder
    /// ends at the rule id and two otherwise-identical proposals must still be
    /// totally ordered for replay to be deterministic.
    RuleId
);

/// The name a check uses on the wire.
///
/// Always `"{responsibility_id}__{check_id}"` — a double underscore, never a dot.
/// Responsibility ids contain dots (`core.completion`), so a dot-joined key could
/// not be split back into its parts unambiguously.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CheckKey(String);

/// The separator between the responsibility id and the check id in a [`CheckKey`].
pub const CHECK_KEY_SEPARATOR: &str = "__";

impl CheckKey {
    #[must_use]
    pub fn new(responsibility: &ResponsibilityId, check: &CheckId) -> Self {
        Self(format!(
            "{}{CHECK_KEY_SEPARATOR}{}",
            responsibility.as_str(),
            check.as_str()
        ))
    }

    /// Parse a wire key back into its parts.
    ///
    /// Splits on the **last** separator, so a check id containing the separator
    /// would still resolve correctly; responsibility ids never contain it.
    #[must_use]
    pub fn split(&self) -> Option<(&str, &str)> {
        self.0.rsplit_once(CHECK_KEY_SEPARATOR)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CheckKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for CheckKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CheckKey({:?})", self.0)
    }
}

impl From<&str> for CheckKey {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for CheckKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}
