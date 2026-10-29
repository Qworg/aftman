//! How an ensemble escalation reaches policy (`docs/06-ensemble.md` §6, and the priority
//! table in `docs/05-responsibilities.md` §6.3).
//!
//! The ladder decides that a check needs a person — a value held in band, or contested,
//! for `consecutive_contests_before_human` cycles, or holds caused by a spent opinion
//! budget — and records the question on the check (`CheckRecord::escalation`). It does
//! not decide what the run does about it. That is policy's job (hard rule 1), so every
//! escalation of one cycle becomes **one** `Escalate` *proposal*, at
//! [`priority::ESCALATE_ENSEMBLE`], arbitrated with everything else.
//!
//! One proposal, not one per check: two held conjuncts are one question about whether the
//! work is done, and a run parks on one escalation at a time anyway.

use crate::directive::{Directive, Proposal, Stakes, priority};
use crate::ids::CheckKey;
use crate::policy::PolicyInput;
use crate::responsibility::ResponsibilityRegistry;

use super::resolve::{EnsembleOutcome, REFRESH_ERROR_CAP_BYTES, cap_bytes};

/// The pseudo-responsibility an ensemble escalation is proposed under.
pub const ENSEMBLE_RESPONSIBILITY: &str = "core.ensemble";
/// The rule id of the coalesced escalation.
pub const ESCALATE_RULE: &str = "core.ensemble:escalate";
/// Cap on the coalesced question. Each check's own question is capped at 2 KiB.
pub const ESCALATION_ASK_CAP_BYTES: usize = 8192;
/// The prefix of the proposal's `reason`, by which a runtime recognises an ensemble
/// escalation when it records one.
pub const ESCALATION_REASON_PREFIX: &str = "ensemble:";

impl EnsembleOutcome {
    /// The checks escalated this cycle, in key order.
    #[must_use]
    pub fn escalated_checks(&self) -> Vec<CheckKey> {
        self.escalations().map(|(key, _)| key.clone()).collect()
    }

    /// Every escalation of this cycle as one `Escalate` proposal, or `None`.
    ///
    /// Its question lists each check's own question; its reason names the checks. Built
    /// only from the check keys and the questions the ladder wrote, which are themselves
    /// built from keys and numbers, never from model or repository text.
    #[must_use]
    pub fn escalation_proposal(&self) -> Option<Proposal> {
        let escalations: Vec<(&CheckKey, &str)> = self.escalations().collect();
        if escalations.is_empty() {
            return None;
        }
        let keys = escalations
            .iter()
            .map(|(key, _)| format!("`{key}`"))
            .collect::<Vec<_>>()
            .join(", ");
        let reason = cap_bytes(
            &format!("{ESCALATION_REASON_PREFIX} a person must decide about {keys}"),
            REFRESH_ERROR_CAP_BYTES,
        );
        let ask = if let [(_, ask)] = escalations.as_slice() {
            (*ask).to_owned()
        } else {
            let mut ask = format!(
                "{} checks cannot be acted on until a person decides.",
                escalations.len()
            );
            for (index, (_, question)) in escalations.iter().enumerate() {
                ask.push_str(&format!("\n\n{}. {question}", index + 1));
            }
            ask
        };
        Some(Proposal::new(
            ENSEMBLE_RESPONSIBILITY,
            ESCALATE_RULE,
            Directive::Escalate {
                reason,
                ask: cap_bytes(&ask, ESCALATION_ASK_CAP_BYTES),
            },
            priority::ESCALATE_ENSEMBLE,
            Stakes::High,
        ))
    }

    /// Whether `directive`, as selected, is this cycle's ensemble escalation.
    #[must_use]
    pub fn is_escalation(&self, directive: &Directive) -> bool {
        self.escalation_proposal()
            .is_some_and(|proposal| proposal.directive == *directive)
    }
}

/// The cycle's proposals: every responsibility's, plus the ensemble's escalation.
///
/// `input.checks` must be `outcome.checks`, the resolved set: that is what the
/// escalation's holds are part of.
#[must_use]
pub fn propose(
    registry: &ResponsibilityRegistry,
    input: &PolicyInput<'_>,
    outcome: &EnsembleOutcome,
) -> Vec<Proposal> {
    let mut proposals = registry.propose(input);
    proposals.extend(outcome.escalation_proposal());
    proposals
}
