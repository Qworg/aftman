//! The `[ensemble]` settings and the per-run counters the resolver keeps.
//!
//! Loading these from a file or flags is the daemon's job; the type, its defaults and
//! its validation live here so that every loader refuses the same things.
//! See `docs/06-ensemble.md` §7 and §12.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::assessment::{DEFAULT_UNCERTAINTY_MARGIN, Rung};
use crate::ids::{AssessmentSeq, CheckKey};

/// Default for [`EnsembleSettings::confidence_floor`].
pub const DEFAULT_CONFIDENCE_FLOOR: f32 = 0.60;
/// Default for [`EnsembleSettings::framing_tolerance`].
pub const DEFAULT_FRAMING_TOLERANCE: f32 = 0.15;
/// Default for [`EnsembleSettings::max_opinions_per_run`].
pub const DEFAULT_MAX_OPINIONS_PER_RUN: u32 = 20;
/// Default for [`EnsembleSettings::max_evidence_refreshes_per_check`].
pub const DEFAULT_MAX_EVIDENCE_REFRESHES_PER_CHECK: u32 = 3;
/// Default for [`EnsembleSettings::opinion_timeout_seconds`].
pub const DEFAULT_OPINION_TIMEOUT_SECONDS: u64 = 120;
/// Default for [`EnsembleSettings::per_check_cooldown_cycles`].
pub const DEFAULT_PER_CHECK_COOLDOWN_CYCLES: u64 = 10;
/// Default for [`EnsembleSettings::consecutive_contests_before_human`].
pub const DEFAULT_CONSECUTIVE_CONTESTS_BEFORE_HUMAN: u32 = 2;

/// The `[ensemble]` table.
///
/// Off by default. With `enabled = false` a rule fires at exactly `p >= t`, as upstream
/// does, and nothing else in this module has any effect.
///
/// There is deliberately no key that lets an opinion enable a directive
/// (`docs/06-ensemble.md` §2.2): the correct value would be `false`, and a knob invites
/// someone to set it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct EnsembleSettings {
    pub enabled: bool,
    /// `|p - t| < margin` is in band, and held.
    pub uncertainty_margin: f32,
    /// A `Choice`/`Score` answer whose reported confidence is below this triggers
    /// `LowConfidence`.
    pub confidence_floor: f32,
    /// R2: `|p(X) - (1 - p(not X))| <= framing_tolerance` confirms; otherwise contests.
    pub framing_tolerance: f32,
    /// Opinion calls (R1/R2) per run. Evidence refreshes are not opinions and do not
    /// count against it.
    pub max_opinions_per_run: u32,
    /// R0 refreshes per check per run.
    pub max_evidence_refreshes_per_check: u32,
    /// How long one opinion may take. Enforced by the source that makes the call.
    pub opinion_timeout_seconds: u64,
    /// At most one opinion per check in this many assessment cycles.
    pub per_check_cooldown_cycles: u64,
    /// A check held or contested in this many consecutive prior cycles escalates
    /// instead of being asked about again.
    pub consecutive_contests_before_human: u32,
    /// The rungs this run may use, in ladder order.
    pub rungs: Vec<Rung>,
}

impl Default for EnsembleSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            uncertainty_margin: DEFAULT_UNCERTAINTY_MARGIN,
            confidence_floor: DEFAULT_CONFIDENCE_FLOOR,
            framing_tolerance: DEFAULT_FRAMING_TOLERANCE,
            max_opinions_per_run: DEFAULT_MAX_OPINIONS_PER_RUN,
            max_evidence_refreshes_per_check: DEFAULT_MAX_EVIDENCE_REFRESHES_PER_CHECK,
            opinion_timeout_seconds: DEFAULT_OPINION_TIMEOUT_SECONDS,
            per_check_cooldown_cycles: DEFAULT_PER_CHECK_COOLDOWN_CYCLES,
            consecutive_contests_before_human: DEFAULT_CONSECUTIVE_CONTESTS_BEFORE_HUMAN,
            rungs: Self::available_rungs().to_vec(),
        }
    }
}

/// A configuration the resolver must not run with.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum EnsembleSettingsError {
    #[error(
        "ensemble.enabled = true with an empty `rungs` list is a configuration error, not a no-op"
    )]
    NoRungs,
    #[error("ensemble.rungs lists `{0}` more than once")]
    DuplicateRung(&'static str),
    #[error(
        "ensemble.rungs lists `{0}`, which is not available in this release \
         (the independent judge is ensemble stage S3)"
    )]
    UnavailableRung(&'static str),
    #[error("ensemble.{key} = {value} is outside {range}")]
    OutOfRange {
        key: &'static str,
        value: String,
        range: &'static str,
    },
}

impl EnsembleSettings {
    /// The rungs this release implements, in ladder order.
    ///
    /// `independent_judge` (R1) is absent on purpose: it is stage S3, and there is no
    /// code path for it. Listing it is refused by [`EnsembleSettings::validate`] rather
    /// than silently ignored, so a configuration never claims a protection it lacks.
    #[must_use]
    pub const fn available_rungs() -> &'static [Rung] {
        &[Rung::RefreshEvidence, Rung::ReframeSameModel, Rung::Human]
    }

    /// Whether a rung is enabled for this run.
    #[must_use]
    pub fn has(&self, rung: Rung) -> bool {
        self.rungs.contains(&rung)
    }

    /// Refuse a configuration the resolver cannot honour.
    ///
    /// Numeric bounds are checked whether or not the ensemble is enabled, so that turning
    /// it on later cannot reveal a typo that was accepted while it was off.
    pub fn validate(&self) -> Result<(), EnsembleSettingsError> {
        let unit = |key: &'static str, value: f32| {
            if value.is_finite() && (0.0..=1.0).contains(&value) {
                Ok(())
            } else {
                Err(EnsembleSettingsError::OutOfRange {
                    key,
                    value: value.to_string(),
                    range: "0..=1",
                })
            }
        };
        unit("uncertainty_margin", self.uncertainty_margin)?;
        unit("confidence_floor", self.confidence_floor)?;
        unit("framing_tolerance", self.framing_tolerance)?;
        if self.consecutive_contests_before_human == 0 {
            return Err(EnsembleSettingsError::OutOfRange {
                key: "consecutive_contests_before_human",
                value: "0".to_owned(),
                range: "1..",
            });
        }
        if self.opinion_timeout_seconds == 0 {
            return Err(EnsembleSettingsError::OutOfRange {
                key: "opinion_timeout_seconds",
                value: "0".to_owned(),
                range: "1..",
            });
        }
        for (index, rung) in self.rungs.iter().enumerate() {
            if !Self::available_rungs().contains(rung) {
                return Err(EnsembleSettingsError::UnavailableRung(rung_name(*rung)));
            }
            if self.rungs[..index].contains(rung) {
                return Err(EnsembleSettingsError::DuplicateRung(rung_name(*rung)));
            }
        }
        if self.enabled && self.rungs.is_empty() {
            return Err(EnsembleSettingsError::NoRungs);
        }
        Ok(())
    }
}

/// The configuration spelling of a rung.
#[must_use]
pub const fn rung_name(rung: Rung) -> &'static str {
    match rung {
        Rung::RefreshEvidence => "refresh_evidence",
        Rung::ReframeSameModel => "reframe_same_model",
        Rung::IndependentJudge => "independent_judge",
        Rung::Human => "human",
    }
}

/// Per-run counters the resolver reads and returns updated.
///
/// Lives in [`crate::run::RunState`], not in [`crate::observation::Counters`]: the latter
/// is serialized into the model-facing observation, and how often the supervisor has
/// second-guessed the model is not something the model should be shown.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct EnsembleCounters {
    /// Opinion calls made so far this run, against `max_opinions_per_run`.
    pub opinions_used: u32,
    /// Keyed by check. A check appears once the ensemble has looked at it.
    pub checks: BTreeMap<CheckKey, CheckCounters>,
}

impl EnsembleCounters {
    /// The counters for one check, or zeros.
    #[must_use]
    pub fn check(&self, key: &CheckKey) -> CheckCounters {
        self.checks.get(key).copied().unwrap_or_default()
    }
}

/// Per-check counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CheckCounters {
    /// R0 refreshes attempted for this check this run, successful or not.
    pub refreshes: u32,
    /// The most recent cycle an opinion was requested for this check.
    pub last_opinion: Option<OpinionMark>,
    /// Consecutive cycles, up to and including the last resolved one, in which this
    /// check ended not permitted (held in band, contested, or unresolved).
    pub consecutive_held: u32,
}

/// When an opinion was last bought for a check, and whether it contested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpinionMark {
    pub cycle: AssessmentSeq,
    pub contested: bool,
}

#[cfg(test)]
mod tests {
    use super::{EnsembleCounters, EnsembleSettings, EnsembleSettingsError};
    use crate::assessment::Rung;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn defaults_match_the_documented_table() {
        let s = EnsembleSettings::default();
        assert!(!s.enabled, "off by default");
        assert!((s.uncertainty_margin - 0.15).abs() < f32::EPSILON);
        assert!((s.confidence_floor - 0.60).abs() < f32::EPSILON);
        assert!((s.framing_tolerance - 0.15).abs() < f32::EPSILON);
        assert_eq!(s.max_opinions_per_run, 20);
        assert_eq!(s.max_evidence_refreshes_per_check, 3);
        assert_eq!(s.opinion_timeout_seconds, 120);
        assert_eq!(s.per_check_cooldown_cycles, 10);
        assert_eq!(s.consecutive_contests_before_human, 2);
        assert_eq!(
            s.rungs,
            vec![Rung::RefreshEvidence, Rung::ReframeSameModel, Rung::Human]
        );
        assert_eq!(s.validate(), Ok(()));
    }

    #[test]
    fn enabled_with_no_rungs_is_an_error_not_a_no_op() {
        let s = EnsembleSettings {
            enabled: true,
            rungs: Vec::new(),
            ..EnsembleSettings::default()
        };
        assert_eq!(s.validate(), Err(EnsembleSettingsError::NoRungs));
        // Disabled with no rungs is fine: nothing will run.
        let off = EnsembleSettings {
            enabled: false,
            rungs: Vec::new(),
            ..EnsembleSettings::default()
        };
        assert_eq!(off.validate(), Ok(()));
    }

    #[test]
    fn the_independent_judge_is_refused_until_it_exists() {
        let s = EnsembleSettings {
            enabled: true,
            rungs: vec![Rung::RefreshEvidence, Rung::IndependentJudge],
            ..EnsembleSettings::default()
        };
        assert_eq!(
            s.validate(),
            Err(EnsembleSettingsError::UnavailableRung("independent_judge"))
        );
    }

    #[test]
    fn a_duplicated_rung_is_refused() {
        let s = EnsembleSettings {
            rungs: vec![Rung::Human, Rung::Human],
            ..EnsembleSettings::default()
        };
        assert_eq!(
            s.validate(),
            Err(EnsembleSettingsError::DuplicateRung("human"))
        );
    }

    #[test]
    fn out_of_range_numbers_are_refused_even_while_disabled() {
        for bad in [
            EnsembleSettings {
                uncertainty_margin: -0.1,
                ..EnsembleSettings::default()
            },
            EnsembleSettings {
                framing_tolerance: f32::NAN,
                ..EnsembleSettings::default()
            },
            EnsembleSettings {
                confidence_floor: 1.5,
                ..EnsembleSettings::default()
            },
            EnsembleSettings {
                consecutive_contests_before_human: 0,
                ..EnsembleSettings::default()
            },
            EnsembleSettings {
                opinion_timeout_seconds: 0,
                ..EnsembleSettings::default()
            },
        ] {
            assert!(
                matches!(
                    bad.validate(),
                    Err(EnsembleSettingsError::OutOfRange { .. })
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn the_table_parses_from_toml_and_refuses_unknown_keys() -> TestResult {
        let s: EnsembleSettings = toml::from_str(
            r#"
enabled = true
uncertainty_margin = 0.1
rungs = ["refresh_evidence", "human"]
"#,
        )?;
        assert!(s.enabled);
        assert!((s.uncertainty_margin - 0.1).abs() < f32::EPSILON);
        assert_eq!(s.max_opinions_per_run, 20, "unset keys keep their default");
        assert_eq!(s.rungs, vec![Rung::RefreshEvidence, Rung::Human]);
        assert!(toml::from_str::<EnsembleSettings>("allow_finish_from_opinion = true").is_err());
        Ok(())
    }

    #[test]
    fn counters_from_an_older_snapshot_decode_as_zero() -> TestResult {
        let counters: EnsembleCounters = serde_json::from_str("{}")?;
        assert_eq!(counters, EnsembleCounters::default());
        let counters: EnsembleCounters =
            serde_json::from_str(r#"{"checks":{"a__b":{"refreshes":2}}}"#)?;
        let check = counters.check(&"a__b".into());
        assert_eq!(check.refreshes, 2);
        assert_eq!(check.consecutive_held, 0);
        assert_eq!(check.last_opinion, None);
        Ok(())
    }
}
