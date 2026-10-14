//! Domain types, responsibilities, and deterministic policy for aftman.
//!
//! This crate is the contract layer. It knows nothing about HTTP, subprocesses,
//! storage, or surfaces — those live in the other crates. In particular:
//!
//! - **No crate may reference the decision model's wire types except `aftman-jev`.**
//!   Here the model appears only as the [`model::DecisionModel`] trait and the
//!   [`model::ModelAnswers`] value type.
//! - **Policy is pure.** [`policy::arbitrate`] takes a fully materialised
//!   [`policy::PolicyInput`] and returns a [`policy::PolicyDecision`]. It reads no
//!   clock, performs no I/O, and contains no randomness, which is what makes a run
//!   replayable from its event log.
//! - **The model estimates; code decides.** No model output becomes an action
//!   without passing through policy.
//!
//! See `docs/02-lld.md` for the normative description of every type here.

#![forbid(unsafe_code)]
// `Condition` is a recursive enum, and its serde derives make the compiler evaluate a
// deeply nested generic requirement. The default limit is too low for that combination.
#![recursion_limit = "512"]

pub mod activity;
pub mod ansi;
pub mod assessment;
pub mod clock;
pub mod condition;
pub mod definitions;
pub mod directive;
pub mod error;
pub mod event;
pub mod evidence;
pub mod ids;
pub mod log;
pub mod model;
pub mod observation;
pub mod policy;
pub mod responsibility;
pub mod routing;
pub mod run;
pub mod testing;
pub mod timefmt;
pub mod worker;

pub use activity::{StreamActivity, StreamUsage, ToolCall};
pub use ansi::strip_control;
pub use assessment::{
    AnswerValue, Assessment, Band, CheckSuppression, Opinion, Resolution, ResolvedCheck, Rung,
    Side, Verdict,
};
pub use clock::{Clock, FixedClock, SystemClock};
pub use condition::{Comparator, Condition, ConditionError};
pub use definitions::{
    DeclarativeResponsibility, DefinitionError, DefinitionSet, ResponsibilityDefinition,
};
pub use directive::{
    Directive, Invariant, Proposal, Stakes, SteerKind, Suppression, SuppressionReason,
};
pub use error::{IdError, ModelError, TransitionError};
pub use event::{EventEnvelope, EventKind};
pub use evidence::{CommandProvider, EvidenceOutcome, EvidencePlan, ProviderCwd, RunWhen};
pub use ids::{
    AssessmentSeq, AttemptId, CheckId, CheckKey, EscalationId, EventSeq, EvidenceId,
    ResponsibilityId, RuleId, RunId, WorkerId,
};
pub use job::{
    ExitCause, FactoryStatus, Job, JobSource, JobSummary, RunConfig, TaskKind, VerifyPolicy,
    WorkerExit, WorkerKind, WorkerStatus,
};
pub use log::{BoundedLog, Stream};
pub use model::{Budget, DecisionModel, ModelAnswers, ModelInfo, Purpose, Question, Usage};
pub use observation::{
    Counters, EventSummary, GitEvidence, Observation, PriorDecision, RepoInstructions, TestResult,
    TestSource, TestStatus, UntrackedExcerpt, WorkerSummary,
};
pub use policy::{PolicyDecision, PolicyInput, arbitrate};
pub use responsibility::{
    Check, DEFAULT_EVIDENCE, Responsibility, ResponsibilityRegistry, Routing,
};
pub use run::{Escalation, EscalationKind, RunFilter, RunHeader, RunState, RunSummary};
pub use worker::{
    ProcessIdentity, ProcessLedger, SteerOutcome, SteerRecord, TerminateReason, Worker, WorkerCtx,
    WorkerEventSink, WorkerOutcome, WorkerRecord, WorkerSignal,
};

pub mod job;
