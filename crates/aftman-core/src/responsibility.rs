//! Responsibilities: the units that own checks and propose directives.
//!
//! A responsibility owns checks (questions for the model) and rules that turn the
//! answers into proposed directives. It does not act, and it has no aggregate score:
//! checks have results, responsibilities do not.
//!
//! **Definition of a check lives in TOML; the rule logic lives here.** See
//! [`crate::definitions`] for why: a rule depends on runtime facts a condition cannot
//! see (whether a verifier has run, whether a steer is still available, whether the
//! attempt budget is spent), so the bundled responsibilities keep their logic in Rust
//! and read their checks from the operator-overridable definitions.

use serde::{Deserialize, Serialize};

use crate::assessment::Band;
use crate::definitions::{DeclarativeResponsibility, DefinitionSet};
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
    /// Must be pure: same input, same proposals. It reads [`PolicyInput`], which
    /// carries a materialised `now` and the active worker record, so nothing here needs
    /// a clock.
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

    /// Build the runtime set from definitions.
    ///
    /// `active` lists the ids that participate: the globals plus whatever the router
    /// activated. A built-in id with no code (impossible for the bundled set, possible
    /// for a user file that reuses one) is skipped rather than silently becoming a
    /// responsibility that can never propose anything.
    pub fn from_definitions(
        definitions: &DefinitionSet,
        active: &[ResponsibilityId],
        steering_grace_seconds: u64,
        max_steers_per_worker: u32,
    ) -> Result<Self, String> {
        let mut built: Vec<Box<dyn Responsibility>> = Vec::new();
        // Iterate the definitions in a stable order so the registry is deterministic,
        // and so `checks()` is deterministic, which the request grouping depends on.
        for (id, definition) in definitions.enabled() {
            if !active.iter().any(|a| a.as_str() == id.as_str()) {
                continue;
            }
            let checks = definition.checks(id);
            let routing = definition.routing();
            let builtin = builtin::for_id(
                id,
                checks,
                routing,
                definition
                    .setting_u64("steering_grace_seconds")
                    .unwrap_or(steering_grace_seconds),
                u32::try_from(
                    definition
                        .setting_u64("max_steers_per_worker")
                        .unwrap_or(u64::from(max_steers_per_worker)),
                )
                .unwrap_or(max_steers_per_worker),
            )?;
            match builtin {
                Some(responsibility) => built.push(responsibility),
                None => built.push(Box::new(
                    DeclarativeResponsibility::new(id, definition)
                        .map_err(|error| format!("{id}: {error}"))?,
                )),
            }
        }
        Ok(Self::new(built))
    }

    /// The bundled set, using its default checks.
    ///
    /// Kept for tests and for callers that do not want to load definitions.
    #[must_use]
    pub fn builtin() -> Self {
        Self::new(builtin::all_default())
    }

    #[must_use]
    pub fn active(&self) -> &[Box<dyn Responsibility>] {
        &self.active
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.active.is_empty()
    }

    /// The ids that participate, in registry order.
    #[must_use]
    pub fn ids(&self) -> Vec<ResponsibilityId> {
        self.active.iter().map(|r| r.id().clone()).collect()
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

    /// Every active check that a rule may read, with its threshold.
    #[must_use]
    pub fn thresholds(&self) -> Vec<(CheckKey, Option<f32>)> {
        self.checks()
            .into_iter()
            .map(|c| (c.key, c.threshold))
            .collect()
    }

    /// Group checks by the evidence providers they select.
    ///
    /// Grouping is the coarsest split that never sends a check evidence it did not ask
    /// for, and it collapses the built-ins to one or two requests rather than one per
    /// check.
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

/// The bundled responsibilities, with the same ids, rule ids, thresholds, and priorities
/// as `docs/05-responsibilities.md` §1 and §6.3.
pub mod builtin {
    use super::{Check, Responsibility, Routing};
    use crate::directive::{Directive, Proposal, Stakes, SteerKind, priority};
    use crate::ids::{CheckId, CheckKey, ResponsibilityId, RuleId};
    use crate::policy::PolicyInput;

    pub const COMPLETION: &str = "core.completion";
    pub const VERIFICATION: &str = "core.verification";
    pub const WORKER_HEALTH: &str = "core.worker-health";
    pub const HUMAN_ESCALATION: &str = "core.human-escalation";
    pub const REPOSITORY_INSTRUCTIONS: &str = "repository.instructions";

    /// The check ids each built-in needs to find its logic's inputs.
    pub const COMPLETION_CHECKS: &[&str] = &[
        "implementation_complete",
        "requirements_satisfied",
        "ready_to_finish",
    ];
    pub const VERIFICATION_CHECKS: &[&str] = &["tests_sufficient", "needs_verification"];
    pub const WORKER_HEALTH_CHECKS: &[&str] =
        &["meaningful_progress", "worker_stuck", "work_off_track"];
    pub const HUMAN_ESCALATION_CHECKS: &[&str] = &["needs_human"];
    pub const REPOSITORY_INSTRUCTIONS_CHECKS: &[&str] = &["agents_md_drift"];

    /// Build the code-logic responsibility for one id, if there is one.
    ///
    /// `None` means the id is not a built-in, so the caller should fall back to the
    /// declarative form.
    pub fn for_id(
        id: &str,
        checks: Vec<Check>,
        routing: Routing,
        steering_grace_seconds: u64,
        max_steers_per_worker: u32,
    ) -> Result<Option<Box<dyn Responsibility>>, String> {
        let built: Option<Box<dyn Responsibility>> = match id {
            COMPLETION => Some(Box::new(Completion {
                id: ResponsibilityId::new(id),
                checks,
                routing,
            })),
            VERIFICATION => Some(Box::new(Verification {
                id: ResponsibilityId::new(id),
                checks,
                routing,
            })),
            WORKER_HEALTH => Some(Box::new(WorkerHealth {
                id: ResponsibilityId::new(id),
                checks,
                routing,
                steering_grace_seconds,
                max_steers_per_worker,
            })),
            HUMAN_ESCALATION => Some(Box::new(HumanEscalation {
                id: ResponsibilityId::new(id),
                checks,
                routing,
            })),
            REPOSITORY_INSTRUCTIONS => Some(Box::new(RepositoryInstructions {
                id: ResponsibilityId::new(id),
                checks,
                routing,
            })),
            _ => None,
        };
        // A built-in whose logic needs a check the definition removed would fail at
        // assessment time in a way that looks like a model problem. Refuse at
        // construction instead, and say which check is missing.
        if let Some(responsibility) = &built {
            let required: &[&str] = match id {
                COMPLETION => COMPLETION_CHECKS,
                VERIFICATION => VERIFICATION_CHECKS,
                WORKER_HEALTH => WORKER_HEALTH_CHECKS,
                HUMAN_ESCALATION => HUMAN_ESCALATION_CHECKS,
                _ => REPOSITORY_INSTRUCTIONS_CHECKS,
            };
            for needed in required {
                if !responsibility
                    .checks()
                    .iter()
                    .any(|check| check.id.as_str() == *needed)
                {
                    return Err(format!(
                        "`{id}` needs a check named `{needed}`; the definition does not declare it"
                    ));
                }
            }
        }
        Ok(built)
    }

    /// Every bundled responsibility with its default checks.
    #[must_use]
    pub fn all_default() -> Vec<Box<dyn Responsibility>> {
        let build = |id: &str, checks: Vec<Check>| -> Box<dyn Responsibility> {
            for_id(id, checks, Routing::global(), 30, 1)
                .ok()
                .flatten()
                .unwrap_or_else(|| {
                    Box::new(Completion {
                        id: ResponsibilityId::new(id),
                        checks: Vec::new(),
                        routing: Routing::global(),
                    })
                })
        };
        vec![
            build(COMPLETION, default_checks("core.completion")),
            build(VERIFICATION, default_checks("core.verification")),
            build(WORKER_HEALTH, default_checks("core.worker-health")),
            build(HUMAN_ESCALATION, default_checks("core.human-escalation")),
            build(
                REPOSITORY_INSTRUCTIONS,
                default_checks("repository.instructions"),
            ),
        ]
    }

    /// The bundled definitions' checks, or an empty list if they fail to load.
    ///
    /// Loading is compiled in and validated by the definition tests, so a failure here
    /// would be a build-time mistake rather than a runtime one.
    pub fn default_checks(id: &str) -> Vec<Check> {
        crate::definitions::DefinitionSet::bundled()
            .ok()
            .and_then(|set| {
                set.definitions
                    .get(id)
                    .map(|definition| definition.checks(id))
            })
            .unwrap_or_default()
    }

    fn key(responsibility: &str, check: &str) -> CheckKey {
        CheckKey::new(&ResponsibilityId::new(responsibility), &CheckId::new(check))
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

    fn check_for<'a>(checks: &'a [Check], id: &str) -> Option<&'a Check> {
        checks.iter().find(|check| check.id.as_str() == id)
    }

    fn threshold_for(checks: &[Check], id: &str) -> f32 {
        // The definition owns the threshold; the logic only needs a default for the
        // case where a check declares none.
        check_for(checks, id)
            .and_then(|check| check.threshold)
            .unwrap_or(0.75)
    }

    /// `core.completion`
    #[derive(Debug)]
    pub struct Completion {
        pub id: ResponsibilityId,
        pub checks: Vec<Check>,
        pub routing: Routing,
    }

    impl Responsibility for Completion {
        fn id(&self) -> &ResponsibilityId {
            &self.id
        }

        fn checks(&self) -> &[Check] {
            &self.checks
        }

        fn routing(&self) -> &Routing {
            &self.routing
        }

        fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
            if input.active_worker.is_some() {
                return Vec::new();
            }
            let ready = input.check_ge(
                &key(COMPLETION, "ready_to_finish"),
                threshold_for(&self.checks, "ready_to_finish"),
            );
            let requirements = input.check_ge(
                &key(COMPLETION, "requirements_satisfied"),
                threshold_for(&self.checks, "requirements_satisfied"),
            );
            let tests = input.check_ge(
                &key(VERIFICATION, "tests_sufficient"),
                threshold_for(&self.checks, "tests_sufficient"),
            );
            // `needs_verification < t` is a *less-than*, so it goes through `check_lt`:
            // the direction-agnostic `permitted` matters here, and an in-band
            // needs_verification must not satisfy this disjunct.
            let verification_resolved = input.counters.verifier_completed
                || input.check_lt(
                    &key(VERIFICATION, "needs_verification"),
                    threshold_for(&self.checks, "needs_verification"),
                );

            if ready && requirements && tests && verification_resolved {
                return vec![proposal(
                    &self.id,
                    "core.completion:thresholds-satisfied",
                    Directive::Finish {
                        reason: "completion thresholds satisfied".into(),
                    },
                    priority::FINISH,
                )];
            }

            if input.counters.workers_run > 0 && input.last_directive_was_stop() {
                return vec![proposal(
                    &self.id,
                    "core.completion:retry-stopped",
                    Directive::Retry {
                        reason: "retrying stopped worker with a fresh agent".into(),
                    },
                    priority::RETRY_STOPPED,
                )];
            }

            vec![proposal(
                &self.id,
                "core.completion:work-remains",
                Directive::StartWorker {
                    reason: "meaningful implementation work remains".into(),
                },
                priority::START_WORKER_COMPLETION,
            )]
        }
    }

    /// `core.verification`
    #[derive(Debug)]
    pub struct Verification {
        pub id: ResponsibilityId,
        pub checks: Vec<Check>,
        pub routing: Routing,
    }

    impl Responsibility for Verification {
        fn id(&self) -> &ResponsibilityId {
            &self.id
        }

        fn checks(&self) -> &[Check] {
            &self.checks
        }

        fn routing(&self) -> &Routing {
            &self.routing
        }

        fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
            // A verifier is only warranted once implementation looks complete, and never
            // twice.
            if input.active_worker.is_some() || input.counters.verifiers_run > 0 {
                return Vec::new();
            }
            let needed = input.check_ge(
                &key(VERIFICATION, "needs_verification"),
                threshold_for(&self.checks, "needs_verification"),
            );
            let implemented = input.check_ge(
                &key(COMPLETION, "implementation_complete"),
                threshold_for(&self.checks, "implementation_complete"),
            );
            if needed && implemented {
                return vec![proposal(
                    &self.id,
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
    #[derive(Debug)]
    pub struct WorkerHealth {
        pub id: ResponsibilityId,
        pub checks: Vec<Check>,
        pub routing: Routing,
        pub steering_grace_seconds: u64,
        pub max_steers_per_worker: u32,
    }

    impl Responsibility for WorkerHealth {
        fn id(&self) -> &ResponsibilityId {
            &self.id
        }

        fn checks(&self) -> &[Check] {
            &self.checks
        }

        fn routing(&self) -> &Routing {
            &self.routing
        }

        fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
            if input.active_worker.is_none() {
                return Vec::new();
            }
            // After a steer, give the worker the grace window to act before judging it
            // again. A supervisor that steers every five seconds is noise in the
            // worker's context window.
            let grace = std::time::Duration::from_secs(self.steering_grace_seconds);
            if input.steered_within(grace) {
                return vec![proposal(
                    &self.id,
                    "core.worker-health:grace",
                    Directive::Continue,
                    priority::STOP_OR_STEER_WORKER_HEALTH,
                )];
            }

            let mut candidates: Vec<(f32, &str, &str)> = Vec::new();
            let off_track = threshold_for(&self.checks, "work_off_track");
            if let Some(p) = input.check_value(&key(WORKER_HEALTH, "work_off_track"))
                && input.check_ge(&key(WORKER_HEALTH, "work_off_track"), off_track)
            {
                candidates.push((
                    p,
                    "core.worker-health:off-track",
                    "active worker appears off track",
                ));
            }
            let stuck = threshold_for(&self.checks, "worker_stuck");
            if let Some(p) = input.check_value(&key(WORKER_HEALTH, "worker_stuck"))
                && input.check_ge(&key(WORKER_HEALTH, "worker_stuck"), stuck)
            {
                candidates.push((p, "core.worker-health:stuck", "active worker appears stuck"));
            }
            // Highest probability wins; ties broken by rule id for determinism.
            candidates.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(b.1)));
            let Some((_, rule, reason)) = candidates.first() else {
                return Vec::new();
            };

            // `Steer` if the backend supports it and budget remains, else `Stop`. This is
            // where a non-steerable backend degrades automatically, with the same
            // thresholds.
            let directive = if input.steering_available_with_limit(self.max_steers_per_worker) {
                Directive::Steer {
                    kind: SteerKind::Nudge,
                    reason: (*reason).to_owned(),
                }
            } else {
                Directive::Stop {
                    reason: (*reason).to_owned(),
                }
            };
            vec![proposal(
                &self.id,
                rule,
                directive,
                priority::STOP_OR_STEER_WORKER_HEALTH,
            )]
        }
    }

    /// `core.human-escalation`
    #[derive(Debug)]
    pub struct HumanEscalation {
        pub id: ResponsibilityId,
        pub checks: Vec<Check>,
        pub routing: Routing,
    }

    impl Responsibility for HumanEscalation {
        fn id(&self) -> &ResponsibilityId {
            &self.id
        }

        fn checks(&self) -> &[Check] {
            &self.checks
        }

        fn routing(&self) -> &Routing {
            &self.routing
        }

        fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
            if !input.check_ge(
                &key(HUMAN_ESCALATION, "needs_human"),
                threshold_for(&self.checks, "needs_human"),
            ) {
                return Vec::new();
            }
            vec![proposal(
                &self.id,
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
    #[derive(Debug)]
    pub struct RepositoryInstructions {
        pub id: ResponsibilityId,
        pub checks: Vec<Check>,
        pub routing: Routing,
    }

    impl Responsibility for RepositoryInstructions {
        fn id(&self) -> &ResponsibilityId {
            &self.id
        }

        fn checks(&self) -> &[Check] {
            &self.checks
        }

        fn routing(&self) -> &Routing {
            &self.routing
        }

        fn propose(&self, input: &PolicyInput<'_>) -> Vec<Proposal> {
            if input.observation.instructions.is_none() || input.active_worker.is_none() {
                return Vec::new();
            }
            if !input.check_ge(
                &key(REPOSITORY_INSTRUCTIONS, "agents_md_drift"),
                threshold_for(&self.checks, "agents_md_drift"),
            ) {
                return Vec::new();
            }
            let directive = if input.steering_available() {
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
                &self.id,
                "repository.instructions:drift",
                directive,
                priority::STOP_OR_STEER_INSTRUCTIONS,
            )]
        }
    }

    /// The condition grammar's view of a built-in, for `doctor`.
    #[must_use]
    pub fn rule_ids() -> Vec<&'static str> {
        vec![
            "core.completion:thresholds-satisfied",
            "core.completion:retry-stopped",
            "core.completion:work-remains",
            "core.verification:warranted",
            "core.worker-health:stuck",
            "core.worker-health:off-track",
            "core.worker-health:grace",
            "repository.instructions:drift",
            "core.human-escalation:needs-human",
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::ResponsibilityRegistry;
    use super::builtin::{self, rule_ids};
    use crate::definitions::DefinitionSet;
    use crate::ids::ResponsibilityId;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn every_builtin_check_key_uses_the_double_underscore_form() -> TestResult {
        let registry = ResponsibilityRegistry::builtin();
        for check in registry.checks() {
            assert!(
                check.key.as_str().contains("__"),
                "{} must use the `__` wire form",
                check.key
            );
            let (responsibility, check_id) = check.key.split().ok_or("a splittable key")?;
            assert!(!responsibility.is_empty() && !check_id.is_empty());
        }
        Ok(())
    }

    #[test]
    fn builtin_rule_ids_are_unique_so_the_tie_break_is_a_total_order() {
        let ids = rule_ids();
        let mut sorted = ids.clone();
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
    fn each_builtin_declares_an_id_routing_and_checks() {
        for r in ResponsibilityRegistry::builtin().active() {
            assert!(!r.id().as_str().is_empty());
            assert!(r.routing().always, "{} should be global", r.id());
            assert!(!r.checks().is_empty(), "{} has no checks", r.id());
        }
    }

    #[test]
    fn the_builtin_registry_sources_its_checks_from_the_definitions() -> TestResult {
        // The point of the definitions layer: an override reaches the runtime set.
        let mut set = DefinitionSet::bundled()?;
        let dir = tempfile::tempdir()?;
        // Every check the completion logic reads is kept; one threshold is changed.
        // Dropping a needed check is refused, and that is asserted separately.
        std::fs::write(
            dir.path().join("core.completion.toml"),
            "[checks.implementation_complete]\ninstructions = \"a\"\nmin_threshold = 0.75\n\n[checks.requirements_satisfied]\ninstructions = \"b\"\nmin_threshold = 0.75\n\n[checks.ready_to_finish]\ninstructions = \"changed\"\nmin_threshold = 0.90\n",
        )?;
        set.overlay_dir(dir.path())?;

        let active = vec![ResponsibilityId::new("core.completion")];
        let registry = ResponsibilityRegistry::from_definitions(&set, &active, 30, 1)
            .map_err(|e| e.to_string())?;
        let checks = registry.checks();
        assert_eq!(checks.len(), 3);
        let ready = checks
            .iter()
            .find(|check| check.id.as_str() == "ready_to_finish")
            .ok_or("ready_to_finish")?;
        assert!((ready.threshold.unwrap_or_default() - 0.90).abs() < f32::EPSILON);
        Ok(())
    }

    #[test]
    fn a_builtin_missing_a_check_its_logic_needs_is_refused() -> TestResult {
        // Otherwise the failure surfaces at assessment time and looks like a model
        // problem rather than a configuration one.
        let mut set = DefinitionSet::bundled()?;
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("core.verification.toml"),
            "[checks.tests_sufficient]\ninstructions = \"?\"\nmin_threshold = 0.75\n",
        )?;
        set.overlay_dir(dir.path())?;
        let active = vec![ResponsibilityId::new("core.verification")];
        let error = ResponsibilityRegistry::from_definitions(&set, &active, 30, 1)
            .err()
            .ok_or("must refuse")?;
        assert!(error.contains("needs_verification"), "{error}");
        Ok(())
    }

    #[test]
    fn only_the_requested_responsibilities_are_activated() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let active = vec![
            ResponsibilityId::new("core.completion"),
            ResponsibilityId::new("core.worker-health"),
        ];
        let registry = ResponsibilityRegistry::from_definitions(&set, &active, 30, 1)
            .map_err(|e| e.to_string())?;
        assert_eq!(registry.ids().len(), 2);
        assert_eq!(registry.ids()[0].as_str(), "core.completion");
        assert_eq!(registry.ids()[1].as_str(), "core.worker-health");
        Ok(())
    }

    #[test]
    fn a_user_authored_responsibility_is_built_declaratively() -> TestResult {
        let mut set = DefinitionSet::bundled()?;
        let dir = tempfile::tempdir()?;
        std::fs::write(
            dir.path().join("my.quality.toml"),
            "[checks.a]\ninstructions = \"?\"\nmin_threshold = 0.5\n\n[[rules]]\nid = \"my.quality:low\"\nwhen = \"my.quality__a < 0.5\"\npropose = \"start-worker\"\n",
        )?;
        set.overlay_dir(dir.path())?;
        let active = vec![ResponsibilityId::new("my.quality")];
        let registry = ResponsibilityRegistry::from_definitions(&set, &active, 30, 1)
            .map_err(|e| e.to_string())?;
        assert_eq!(registry.ids().len(), 1);
        assert_eq!(registry.checks().len(), 1);
        Ok(())
    }

    #[test]
    fn thresholds_and_routing_come_from_the_definition() -> TestResult {
        let set = DefinitionSet::bundled()?;
        let active = vec![
            ResponsibilityId::new("core.completion"),
            ResponsibilityId::new("quality.documentation"),
        ];
        let registry = ResponsibilityRegistry::from_definitions(&set, &active, 30, 1)
            .map_err(|e| e.to_string())?;
        let documentation = registry
            .active()
            .iter()
            .find(|r| r.id().as_str() == "quality.documentation")
            .ok_or("the documentation responsibility must be built")?;
        assert!(!documentation.routing().always);
        assert!((documentation.routing().threshold - 0.70).abs() < f32::EPSILON);
        let thresholds = registry.thresholds();
        let declared = thresholds
            .iter()
            .find(|(key, _)| key.as_str() == "core.completion__ready_to_finish")
            .and_then(|(_, threshold)| *threshold)
            .ok_or("ready_to_finish must declare a threshold")?;
        assert!((declared - 0.75).abs() < f32::EPSILON);
        Ok(())
    }

    #[test]
    fn completion_declares_the_three_documented_checks() -> TestResult {
        let checks = builtin::default_checks("core.completion");
        // Compared as a set: the checks live in a `BTreeMap`, so they come back
        // alphabetically rather than in declaration order. The wire form is a JSON
        // object keyed by name, so order carries no meaning.
        let mut ids: Vec<&str> = checks.iter().map(|c| c.id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![
                "implementation_complete",
                "ready_to_finish",
                "requirements_satisfied"
            ]
        );
        assert!(checks.iter().all(|c| c.threshold == Some(0.75)));
        Ok(())
    }

    #[test]
    fn verification_thresholds_match_the_docs() -> TestResult {
        let checks = builtin::default_checks("core.verification");
        // Sorted by name; the threshold is not ordered, so it is compared after.
        let mut by_id: Vec<&str> = checks.iter().map(|c| c.id.as_str()).collect();
        by_id.sort_unstable();
        let thresholds = |check: &str| {
            checks
                .iter()
                .find(|c| c.id.as_str() == check)
                .and_then(|c| c.threshold)
        };
        assert_eq!(by_id, vec!["needs_verification", "tests_sufficient"]);
        assert_eq!(thresholds("needs_verification"), Some(0.65));
        assert_eq!(thresholds("tests_sufficient"), Some(0.75));
        Ok(())
    }

    #[test]
    fn worker_health_progress_has_no_threshold() -> TestResult {
        let checks = builtin::default_checks("core.worker-health");
        let progress = checks
            .iter()
            .find(|c| c.id.as_str() == "meaningful_progress")
            .ok_or("meaningful_progress")?;
        assert_eq!(progress.threshold, None, "it cannot drive a threshold rule");
        assert_eq!(
            checks.iter().filter(|c| c.threshold.is_some()).count(),
            2,
            "worker_stuck and work_off_track only"
        );
        Ok(())
    }

    #[test]
    fn drift_check_spells_out_the_not_applicable_case() -> TestResult {
        let checks = builtin::default_checks("repository.instructions");
        let instructions = match &checks[0].question {
            crate::model::Question::Noul { instructions } => instructions.clone(),
            other => panic!("expected a noul, got {other:?}"),
        };
        assert!(
            instructions.contains("Answer no when no AGENTS.md instructions are present"),
            "the not-applicable case must be stated explicitly"
        );
        Ok(())
    }
}
