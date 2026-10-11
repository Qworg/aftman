//! Responsibilities: the units that own checks and propose directives.
//!
//! A responsibility owns checks (questions for the model) and rules that turn the
//! answers into proposed directives. It does not act, and it has no aggregate
//! score: checks have results, responsibilities do not.
//!
//! In M0 the built-in responsibilities propose in Rust, mirroring the upstream
//! logic. M2 moves the definitions into TOML with the `when` grammar from
//! `docs/05-responsibilities.md` §2.1; the ids, rule ids, thresholds, and
//! priorities here are already the ones the TOML will declare, so that move is a
//! relocation rather than a redesign.

use serde::{Deserialize, Serialize};

use crate::assessment::Band;
use crate::directive::Proposal;
use crate::ids::{CheckId, CheckKey, EvidenceId, ResponsibilityId};
use crate::model::Question;
use crate::policy::PolicyInput;

pub use crate::observation::DEFAULT_EVIDENCE;

/// One question for the model, owned by a responsibility.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Check {
    pub id: CheckId,
    /// The wire key. Always `"{responsibility_id}__{check_id}"`.
    pub key: CheckKey,
    pub question: Question,
    /// Providers this check selects. Empty means [`DEFAULT_EVIDENCE`].
    pub evidence: Vec<EvidenceId>,
    /// The threshold its band is measured against. `None` means the check cannot
    /// drive a threshold rule.
    pub threshold: Option<f32>,
}

impl Check {
    #[must_use]
    pub fn new(
        responsibility: &ResponsibilityId,
        id: &str,
        question: Question,
        threshold: Option<f32>,
    ) -> Self {
        let check_id = CheckId::new(id);
        Self {
            key: CheckKey::new(responsibility, &check_id),
            id: check_id,
            question,
            evidence: Vec::new(),
            threshold,
        }
    }

    #[must_use]
    pub fn with_evidence(mut self, evidence: &[&str]) -> Self {
        self.evidence = evidence.iter().map(|e| EvidenceId::new(*e)).collect();
        self
    }
}

/// Whether a responsibility participates without routing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Routing {
    pub always: bool,
    pub instructions: Option<String>,
    pub threshold: f32,
}

impl Routing {
    #[must_use]
    pub fn global() -> Self {
        Self {
            always: true,
            instructions: None,
            threshold: 0.0,
        }
    }

    #[must_use]
    pub fn conditional(instructions: impl Into<String>, threshold: f32) -> Self {
        Self {
            always: false,
            instructions: Some(instructions.into()),
            threshold,
        }
    }
}

/// A unit of supervision.
pub trait Responsibility: Send + Sync {
    fn id(&self) -> &ResponsibilityId;

    fn checks(&self) -> &[Check];

    fn routing(&self) -> &Routing;

    /// Propose directives from this cycle's resolution.
    ///
    /// Must be pure: same input, same proposals. It reads
    /// [`PolicyInput`], which carries a materialised `now` and the active worker
    /// record, so nothing here needs a clock.
    fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal>;
}

/// The set of responsibilities active for a run.
#[derive(Default)]
pub struct ResponsibilityRegistry {
    active: Vec<Box<dyn Responsibility>>,
}

impl std::fmt::Debug for ResponsibilityRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponsibilityRegistry")
            .field(
                "active",
                &self
                    .active
                    .iter()
                    .map(|r| r.id().to_string())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl ResponsibilityRegistry {
    #[must_use]
    pub fn new(active: Vec<Box<dyn Responsibility>>) -> Self {
        Self { active }
    }

    /// The bundled responsibilities that participate without routing.
    #[must_use]
    pub fn builtin() -> Self {
        Self::new(vec![
            Box::new(builtin::CompletionResponsibility),
            Box::new(builtin::VerificationResponsibility),
            Box::new(builtin::WorkerHealthResponsibility),
            Box::new(builtin::HumanEscalationResponsibility),
            Box::new(builtin::RepositoryInstructionsResponsibility),
        ])
    }

    #[must_use]
    pub fn active(&self) -> &[Box<dyn Responsibility>] {
        &self.active
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    /// Every active check, flattened. These are the questions for one request.
    #[must_use]
    pub fn checks(&self) -> Vec<Check> {
        self.active
            .iter()
            .flat_map(|r| r.checks().iter().cloned())
            .collect()
    }

    /// Every active check key.
    #[must_use]
    pub fn keys(&self) -> Vec<CheckKey> {
        self.active
            .iter()
            .flat_map(|r| r.checks().iter().map(|c| c.key.clone()))
            .collect()
    }

    /// Group checks by the evidence providers they select.
    ///
    /// Grouping is the coarsest split that never sends a check evidence it did not
    /// ask for, and it collapses the built-ins to one or two requests rather than
    /// one per check.
    #[must_use]
    pub fn grouped_by_evidence(&self) -> Vec<(Vec<EvidenceId>, Vec<Check>)> {
        let mut groups: Vec<(Vec<EvidenceId>, Vec<Check>)> = Vec::new();
        for check in self.checks() {
            let selection = if check.evidence.is_empty() {
                DEFAULT_EVIDENCE
                    .iter()
                    .map(|e| EvidenceId::new(*e))
                    .collect()
            } else {
                check.evidence.clone()
            };
            match groups
                .iter_mut()
                .find(|(providers, _)| *providers == selection)
            {
                Some((_, checks)) => checks.push(check),
                None => groups.push((selection, vec![check])),
            }
        }
        groups
    }

    /// All proposals from all active responsibilities, in a deterministic order.
    ///
    /// Sorted by priority before returning so that the order does not depend on
    /// registration order — policy breaks ties by rule id anyway, but a stable
    /// order makes the event log easier to read.
    #[must_use]
    pub fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
        let mut proposals: Vec<Proposal> =
            self.active.iter().flat_map(|r| r.propose(input)).collect();
        proposals.sort_by(|a, b| a.order_key().cmp(&b.order_key()));
        proposals
    }
}

/// Convenience: the ordered providers for a check, as owned strings.
#[must_use]
pub fn providers_for(check: &Check) -> Vec<String> {
    if check.evidence.is_empty() {
        crate::observation::default_evidence()
    } else {
        check.evidence.iter().map(ToString::to_string).collect()
    }
}

/// Whether a check's value is sitting in the band.
#[must_use]
pub fn is_in_band(input: &PolicyInput<'_>, key: &CheckKey) -> bool {
    input
        .checks
        .get(key)
        .is_some_and(|c| c.band == Band::InBand)
}

/// The bundled responsibilities, with the same ids, rule ids, thresholds, and
/// priorities as `docs/05-responsibilities.md` §1 and §6.3.
pub mod builtin {
    use super::{Check, Responsibility, Routing};
    use crate::directive::{Directive, Proposal, Stakes, SteerKind, priority};
    use crate::ids::{CheckId, CheckKey, ResponsibilityId, RuleId};
    use crate::model::Question;
    use crate::policy::PolicyInput;

    pub const COMPLETION: &str = "core.completion";
    pub const VERIFICATION: &str = "core.verification";
    pub const WORKER_HEALTH: &str = "core.worker-health";
    pub const HUMAN_ESCALATION: &str = "core.human-escalation";
    pub const REPOSITORY_INSTRUCTIONS: &str = "repository.instructions";

    pub const T_COMPLETE: f32 = 0.75;
    pub const T_REQUIREMENTS: f32 = 0.75;
    pub const T_READY: f32 = 0.75;
    pub const T_TESTS: f32 = 0.75;
    pub const T_NEEDS_VERIFICATION: f32 = 0.65;
    pub const T_STUCK: f32 = 0.80;
    pub const T_OFF_TRACK: f32 = 0.80;
    pub const T_DRIFT: f32 = 0.80;
    pub const T_NEEDS_HUMAN: f32 = 0.80;

    /// `core.completion`
    #[derive(Debug, Default)]
    pub struct CompletionResponsibility;

    impl CompletionResponsibility {
        fn checks() -> Vec<Check> {
            let r = ResponsibilityId::new(COMPLETION);
            vec![
                Check::new(
                    &r,
                    "implementation_complete",
                    Question::noul(
                        "Is the implementation work required by the original job complete?",
                    ),
                    Some(T_COMPLETE),
                ),
                Check::new(
                    &r,
                    "requirements_satisfied",
                    Question::noul(
                        "Does the current repository satisfy the original free-form job as a whole?",
                    ),
                    Some(T_REQUIREMENTS),
                ),
                Check::new(
                    &r,
                    "ready_to_finish",
                    Question::noul("Given all evidence, is the job ready to be declared complete?"),
                    Some(T_READY),
                ),
            ]
        }
    }

    impl Responsibility for CompletionResponsibility {
        fn id(&self) -> &ResponsibilityId {
            static ID: std::sync::OnceLock<ResponsibilityId> = std::sync::OnceLock::new();
            ID.get_or_init(|| ResponsibilityId::new(COMPLETION))
        }

        fn checks(&self) -> &[Check] {
            static CHECKS: std::sync::OnceLock<Vec<Check>> = std::sync::OnceLock::new();
            CHECKS.get_or_init(Self::checks)
        }

        fn routing(&self) -> &Routing {
            static ROUTING: std::sync::OnceLock<Routing> = std::sync::OnceLock::new();
            ROUTING.get_or_init(Routing::global)
        }

        fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
            if input.active_worker.is_some() {
                return Vec::new();
            }
            let ready = input.check_ge(&key("ready_to_finish"), T_READY);
            let requirements = input.check_ge(&key("requirements_satisfied"), T_REQUIREMENTS);
            let tests = input.check_ge(&verification_key("tests_sufficient"), T_TESTS);
            // `needs_verification < t` is a *less-than*, so it goes through
            // `check_lt` — the direction-agnostic `permitted` matters here, and an
            // in-band needs_verification must not satisfy this disjunct.
            let verification_resolved = input.counters.verifier_completed
                || input.check_lt(
                    &verification_key("needs_verification"),
                    T_NEEDS_VERIFICATION,
                );

            if ready && requirements && tests && verification_resolved {
                return vec![proposal(
                    self.id(),
                    "core.completion:thresholds-satisfied",
                    Directive::Finish {
                        reason: "completion thresholds satisfied".into(),
                    },
                    priority::FINISH,
                )];
            }

            if input.counters.workers_run > 0 && input.last_directive_was_stop() {
                return vec![proposal(
                    self.id(),
                    "core.completion:retry-stopped",
                    Directive::Retry {
                        reason: "retrying stopped worker with a fresh agent".into(),
                    },
                    priority::RETRY_STOPPED,
                )];
            }

            vec![proposal(
                self.id(),
                "core.completion:work-remains",
                Directive::StartWorker {
                    reason: "meaningful implementation work remains".into(),
                },
                priority::START_WORKER_COMPLETION,
            )]
        }
    }

    /// `core.verification`
    #[derive(Debug, Default)]
    pub struct VerificationResponsibility;

    impl Responsibility for VerificationResponsibility {
        fn id(&self) -> &ResponsibilityId {
            static ID: std::sync::OnceLock<ResponsibilityId> = std::sync::OnceLock::new();
            ID.get_or_init(|| ResponsibilityId::new(VERIFICATION))
        }

        fn checks(&self) -> &[Check] {
            static CHECKS: std::sync::OnceLock<Vec<Check>> = std::sync::OnceLock::new();
            CHECKS.get_or_init(|| {
                let r = ResponsibilityId::new(VERIFICATION);
                vec![
                    Check::new(
                        &r,
                        "tests_sufficient",
                        Question::noul(
                            "Does the work have sufficient relevant test coverage and passing verification?",
                        ),
                        Some(T_TESTS),
                    ),
                    Check::new(
                        &r,
                        "needs_verification",
                        Question::noul(
                            "Does the current state warrant an independent verification pass before finishing?",
                        ),
                        Some(T_NEEDS_VERIFICATION),
                    ),
                ]
            })
        }

        fn routing(&self) -> &Routing {
            static ROUTING: std::sync::OnceLock<Routing> = std::sync::OnceLock::new();
            ROUTING.get_or_init(Routing::global)
        }

        fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
            // A verifier is only warranted once implementation looks complete, and
            // never twice.
            if input.active_worker.is_some() || input.counters.verifiers_run > 0 {
                return Vec::new();
            }
            let needed = input.check_ge(&key("needs_verification"), T_NEEDS_VERIFICATION);
            let implemented =
                input.check_ge(&completion_key("implementation_complete"), T_COMPLETE);
            if needed && implemented {
                return vec![proposal(
                    self.id(),
                    "core.verification:warranted",
                    Directive::Verify {
                        reason: "independent verification is warranted".into(),
                    },
                    priority::VERIFY,
                )];
            }
            Vec::new()
        }
    }

    /// `core.worker-health`
    #[derive(Debug, Default)]
    pub struct WorkerHealthResponsibility;

    impl Responsibility for WorkerHealthResponsibility {
        fn id(&self) -> &ResponsibilityId {
            static ID: std::sync::OnceLock<ResponsibilityId> = std::sync::OnceLock::new();
            ID.get_or_init(|| ResponsibilityId::new(WORKER_HEALTH))
        }

        fn checks(&self) -> &[Check] {
            static CHECKS: std::sync::OnceLock<Vec<Check>> = std::sync::OnceLock::new();
            CHECKS.get_or_init(|| {
                let r = ResponsibilityId::new(WORKER_HEALTH);
                vec![
                    Check::new(
                        &r,
                        "meaningful_progress",
                        Question::noul(
                            "Is the active or most recent worker making meaningful progress toward the job?",
                        ),
                        None,
                    ),
                    Check::new(
                        &r,
                        "worker_stuck",
                        Question::noul(
                            "Does the active or most recent worker appear stuck, looping, or unable to advance?",
                        ),
                        Some(T_STUCK),
                    ),
                    Check::new(
                        &r,
                        "work_off_track",
                        Question::noul(
                            "Is the current work drifting from the original job or making unrelated changes?",
                        ),
                        Some(T_OFF_TRACK),
                    ),
                ]
            })
        }

        fn routing(&self) -> &Routing {
            static ROUTING: std::sync::OnceLock<Routing> = std::sync::OnceLock::new();
            ROUTING.get_or_init(Routing::global)
        }

        fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
            if input.active_worker.is_none() {
                return Vec::new();
            }
            // After a steer, give the worker the grace window to act before judging
            // it again. A supervisor that steers every five seconds is noise in the
            // worker's context window.
            if input.within_steering_grace() {
                return vec![proposal(
                    self.id(),
                    "core.worker-health:grace",
                    Directive::Continue,
                    priority::STOP_OR_STEER_WORKER_HEALTH,
                )];
            }

            let mut candidates: Vec<(f32, &str, &str)> = Vec::new();
            if let Some(p) = input.check_value(&key("work_off_track")) {
                if input.check_ge(&key("work_off_track"), T_OFF_TRACK) {
                    candidates.push((
                        p,
                        "core.worker-health:off-track",
                        "active worker appears off track",
                    ));
                }
            }
            if let Some(p) = input.check_value(&key("worker_stuck")) {
                if input.check_ge(&key("worker_stuck"), T_STUCK) {
                    candidates.push((p, "core.worker-health:stuck", "active worker appears stuck"));
                }
            }
            // Highest probability wins; ties broken by name for determinism.
            candidates.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(b.1)));
            let Some((_, rule, reason)) = candidates.first() else {
                return Vec::new();
            };

            vec![self.warning(input, rule, reason)]
        }
    }

    impl WorkerHealthResponsibility {
        /// `Steer` if the backend supports it and budget remains, else `Stop`.
        ///
        /// This is where a non-steerable backend degrades automatically, with the
        /// same thresholds.
        fn warning(&self, input: &PolicyInput<'_>, rule: &str, reason: &str) -> Proposal {
            let steerable = input.steering_available();
            let directive = if steerable {
                Directive::Steer {
                    kind: SteerKind::Nudge,
                    reason: reason.to_owned(),
                }
            } else {
                Directive::Stop {
                    reason: reason.to_owned(),
                }
            };
            proposal(
                self.id(),
                rule,
                directive,
                priority::STOP_OR_STEER_WORKER_HEALTH,
            )
        }
    }

    /// `core.human-escalation`
    #[derive(Debug, Default)]
    pub struct HumanEscalationResponsibility;

    impl Responsibility for HumanEscalationResponsibility {
        fn id(&self) -> &ResponsibilityId {
            static ID: std::sync::OnceLock<ResponsibilityId> = std::sync::OnceLock::new();
            ID.get_or_init(|| ResponsibilityId::new(HUMAN_ESCALATION))
        }

        fn checks(&self) -> &[Check] {
            static CHECKS: std::sync::OnceLock<Vec<Check>> = std::sync::OnceLock::new();
            CHECKS.get_or_init(|| {
                let r = ResponsibilityId::new(HUMAN_ESCALATION);
                vec![Check::new(
                    &r,
                    "needs_human",
                    Question::noul(
                        "Does this situation require human judgment, credentials, clarification, or permission?",
                    ),
                    Some(T_NEEDS_HUMAN),
                )]
            })
        }

        fn routing(&self) -> &Routing {
            static ROUTING: std::sync::OnceLock<Routing> = std::sync::OnceLock::new();
            ROUTING.get_or_init(Routing::global)
        }

        fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
            if !input.check_ge(&key("needs_human"), T_NEEDS_HUMAN) {
                return Vec::new();
            }
            vec![proposal(
                self.id(),
                "core.human-escalation:needs-human",
                Directive::Escalate {
                    reason: "the model judges that human input is required".into(),
                    ask: "This run needs a human decision before it can continue.".into(),
                },
                priority::ESCALATE_NEEDS_HUMAN,
            )]
        }
    }

    /// `repository.instructions`
    #[derive(Debug, Default)]
    pub struct RepositoryInstructionsResponsibility;

    impl Responsibility for RepositoryInstructionsResponsibility {
        fn id(&self) -> &ResponsibilityId {
            static ID: std::sync::OnceLock<ResponsibilityId> = std::sync::OnceLock::new();
            ID.get_or_init(|| ResponsibilityId::new(REPOSITORY_INSTRUCTIONS))
        }

        fn checks(&self) -> &[Check] {
            static CHECKS: std::sync::OnceLock<Vec<Check>> = std::sync::OnceLock::new();
            CHECKS.get_or_init(|| {
                let r = ResponsibilityId::new(REPOSITORY_INSTRUCTIONS);
                vec![Check::new(
                    &r,
                    "agents_md_drift",
                    Question::noul(
                        "When agents_md_instructions is present, is the active or most recent worker's \
                         behavior or repository work materially inconsistent with those repository \
                         instructions? Answer no when no AGENTS.md instructions are present or the \
                         evidence is insufficient.",
                    ),
                    Some(T_DRIFT),
                )]
            })
        }

        fn routing(&self) -> &Routing {
            static ROUTING: std::sync::OnceLock<Routing> = std::sync::OnceLock::new();
            ROUTING.get_or_init(Routing::global)
        }

        fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
            if input.observation.instructions.is_none() {
                return Vec::new();
            }
            if input.active_worker.is_none() {
                return Vec::new();
            }
            if !input.check_ge(&key("agents_md_drift"), T_DRIFT) {
                return Vec::new();
            }
            let steerable = input.steering_available();
            let directive = if steerable {
                Directive::Steer {
                    kind: SteerKind::Refocus,
                    reason: "work appears inconsistent with repository instructions".into(),
                }
            } else {
                Directive::Stop {
                    reason: "work appears inconsistent with repository instructions".into(),
                }
            };
            vec![proposal(
                self.id(),
                "repository.instructions:drift",
                directive,
                priority::STOP_OR_STEER_INSTRUCTIONS,
            )]
        }
    }

    fn key(check: &str) -> CheckKey {
        CheckKey::new(&ResponsibilityId::new(COMPLETION), &CheckId::new(check))
    }

    fn verification_key(check: &str) -> CheckKey {
        CheckKey::new(&ResponsibilityId::new(VERIFICATION), &CheckId::new(check))
    }

    fn completion_key(check: &str) -> CheckKey {
        key(check)
    }

    fn proposal(
        responsibility: &ResponsibilityId,
        rule: &str,
        directive: Directive,
        priority: u16,
    ) -> Proposal {
        Proposal::new(
            responsibility.clone(),
            RuleId::new(rule),
            directive,
            priority,
            Stakes::High,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{Responsibility, ResponsibilityRegistry, builtin::*};

    #[test]
    fn every_builtin_check_key_uses_the_double_underscore_form()
    -> Result<(), Box<dyn std::error::Error>> {
        let registry = ResponsibilityRegistry::builtin();
        for check in registry.checks() {
            assert!(
                check.key.as_str().contains("__"),
                "{} must use the `__` wire form",
                check.key
            );
            let (responsibility, check_id) =
                check.key.split().ok_or("a wire key must be splittable")?;
            assert!(!responsibility.is_empty() && !check_id.is_empty());
        }
        Ok(())
    }

    #[test]
    fn builtin_rule_ids_are_unique_so_the_tie_break_is_a_total_order() {
        // Rule ids must be unique across the registry for the policy tie-break to
        // be total. Collect the ids the built-ins can emit.
        let ids = [
            "core.completion:thresholds-satisfied",
            "core.completion:retry-stopped",
            "core.completion:work-remains",
            "core.verification:warranted",
            "core.worker-health:stuck",
            "core.worker-health:off-track",
            "core.worker-health:grace",
            "repository.instructions:drift",
            "core.human-escalation:needs-human",
        ];
        let mut sorted = ids.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "duplicate rule id");
    }

    #[test]
    fn checks_are_grouped_by_evidence_selection() {
        let registry = ResponsibilityRegistry::builtin();
        let groups = registry.grouped_by_evidence();
        // Every built-in selects the default evidence set, so they collapse to one
        // request rather than one request per check.
        assert_eq!(groups.len(), 1, "expected a single evidence group");
        assert_eq!(groups[0].1.len(), registry.checks().len());
    }

    #[test]
    fn each_builtin_declares_an_id_and_routing() {
        for r in ResponsibilityRegistry::builtin().active() {
            assert!(!r.id().as_str().is_empty());
            assert!(r.routing().always, "{} should be global in M0", r.id());
            assert!(!r.checks().is_empty(), "{} has no checks", r.id());
        }
    }

    #[test]
    fn completion_declares_the_three_documented_checks() {
        let r = CompletionResponsibility;
        let ids: Vec<&str> = r.checks().iter().map(|c| c.id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "implementation_complete",
                "requirements_satisfied",
                "ready_to_finish"
            ]
        );
        assert!(r.checks().iter().all(|c| c.threshold == Some(0.75)));
    }

    #[test]
    fn verification_thresholds_match_the_docs() {
        let r = VerificationResponsibility;
        let by_id: Vec<(&str, Option<f32>)> = r
            .checks()
            .iter()
            .map(|c| (c.id.as_str(), c.threshold))
            .collect();
        assert_eq!(
            by_id,
            vec![
                ("tests_sufficient", Some(0.75)),
                ("needs_verification", Some(0.65))
            ]
        );
    }

    #[test]
    fn worker_health_progress_has_no_threshold() -> Result<(), Box<dyn std::error::Error>> {
        let checks = WorkerHealthResponsibility.checks();
        let progress = checks
            .iter()
            .find(|c| c.id.as_str() == "meaningful_progress")
            .ok_or("meaningful_progress must exist")?;
        assert_eq!(progress.threshold, None, "it cannot drive a threshold rule");
        assert_eq!(
            checks.iter().filter(|c| c.threshold.is_some()).count(),
            2,
            "worker_stuck and work_off_track only"
        );
        Ok(())
    }

    #[test]
    fn drift_check_spells_out_the_not_applicable_case() {
        // Without this sentence, a model with no AGENTS.md to compare against emits
        // an uninformative middling probability that trips the threshold.
        let checks = RepositoryInstructionsResponsibility.checks();
        let instructions = match &checks[0].question {
            crate::model::Question::Noul { instructions } => instructions.clone(),
            other => panic!("expected a noul, got {other:?}"),
        };
        assert!(
            instructions.contains("Answer no when no AGENTS.md instructions are present"),
            "the not-applicable case must be stated explicitly"
        );
    }
}
