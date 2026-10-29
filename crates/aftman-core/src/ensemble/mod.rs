//! The ensemble: what happens when a probability is not good enough to act on.
//!
//! Everything here follows from one rule (`docs/06-ensemble.md` §2.2): **nothing a model
//! produces can enable a directive.** Jev's number is never modified; a check carries a
//! boolean `permitted` beside it, and the only things that may set it are Jev's own
//! decisive value ([`crate::assessment::ResolvedCheck::from_measurement`]) and a fresh
//! measurement from an evidence refresh
//! ([`crate::assessment::ResolvedCheck::from_refresh`]). Opinions can only take
//! permission away.

pub mod events;
pub mod resolve;
pub mod settings;
pub mod trigger;

#[cfg(test)]
mod tests;

pub use events::{
    CheckResolvedPayload, EnsembleResolvedPayload, EnsembleTriggeredPayload,
    OpinionRecordedPayload, OpinionRequestedPayload,
};
pub use resolve::{
    CheckRecord, EnsembleLimit, EnsembleOutcome, KeyedOpinion, OpinionKey, OpinionSource,
    ProviderRun, ReframeAnswer, ReframeRequest, RefreshOutcome, RefreshRecord, RefreshRejection,
    RefreshRequest, SourceError, apply_opinions, reframe_verdict,
};
pub use settings::{
    CheckCounters, EnsembleCounters, EnsembleSettings, EnsembleSettingsError, MarkVerdict,
    OpinionMark,
};
pub use trigger::{
    CheckPlan, DecisiveDirectives, EnsembleContext, EnsemblePlan, EnsembleTrigger,
    refresh_providers, sees_command_evidence,
};
