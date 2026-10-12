//! The run loop.
//!
//! Two loops run concurrently, and the design point is that the second one does not
//! stop the first:
//!
//! - the **worker** runs as an independent task, streaming output into a shared
//!   bounded log and signalling as it goes;
//! - the **watcher** wakes on activity, on worker completion, or on an idle tick,
//!   assembles a bounded observation, asks the model, arbitrates, and applies
//!   exactly one directive.
//!
//! Persisted events are an *output* of a cycle, never its input. An earlier design
//! said a cycle begins when a worker event enters the queue while also saying output
//! events are only flushed at a cycle — which is circular, and would have meant the
//! supervisor slept until the worker exited. See `docs/02-lld.md` §7.1.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use time::OffsetDateTime;
use tokio::sync::mpsc;

use aftman_core::assessment::{ResolvedCheck, Rung};
use aftman_core::clock::Clock;
use aftman_core::definitions::DefinitionSet;
use aftman_core::directive::{Directive, Invariant, Proposal, SuppressionReason};
use aftman_core::error::{ModelError, TransitionError};
use aftman_core::event::{EventEnvelope, EventKind};
use aftman_core::evidence::{EvidenceOutcome, EvidencePlan, RunWhen};
use aftman_core::ids::{CheckKey, EscalationId, EventSeq, RunId, WorkerId};
use aftman_core::job::{FactoryStatus, Job, RunConfig, TaskKind, WorkerStatus};
use aftman_core::log::BoundedLog;
use aftman_core::model::{DecisionModel, Purpose, Question};
use aftman_core::observation::{EventSummary, Observation};
use aftman_core::policy::{PolicyInput, arbitrate};
use aftman_core::responsibility::ResponsibilityRegistry;
use aftman_core::routing;
use aftman_core::run::{Escalation, EscalationKind, RunHeader, RunState};
use aftman_core::worker::{
    SteerOutcome, SteerRecord, TerminateReason, Worker, WorkerCtx, WorkerEventSink, WorkerOutcome,
    WorkerRecord, WorkerSignal,
};
use aftman_store::{RunStore, StoreError};

use crate::obs::ObservationBuilder;
use crate::sink::EventSink;

/// Everything the runtime needs. Assembled by the caller so that tests can supply
/// doubles for any part.
pub struct RuntimeDeps {
    pub config: RunConfig,
    pub store: Arc<dyn RunStore>,
    pub model: Arc<dyn DecisionModel>,
    pub worker: Arc<dyn Worker>,
    pub registry: ResponsibilityRegistry,
    /// The declarations routing and check construction read.
    pub definitions: DefinitionSet,
    /// Command evidence providers.
    pub evidence: EvidencePlan,
    /// Scratch space for a provider that runs with `cwd = "temp"`.
    pub evidence_temp_dir: PathBuf,
    pub clock: Arc<dyn Clock>,
    pub sink: Arc<dyn EventSink>,
    /// Whether the uncertainty band holds a rule. Off by default, which restores
    /// plain threshold behaviour exactly.
    pub ensemble_enabled: bool,
    pub uncertainty_margin: f32,
    pub steering_grace: Duration,
}

/// Errors that stop a run for a reason operability can act on.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("repository {0} is not a directory")]
    BadRepo(String),

    #[error("storage error: {0}")]
    Store(#[from] StoreError),

    #[error("model error: {0}")]
    Model(#[from] ModelError),

    #[error("worker error: {0}")]
    Worker(#[from] aftman_core::worker::WorkerError),

    #[error("illegal status transition: {0}")]
    Transition(#[from] TransitionError),

    #[error("responsibility configuration: {0}")]
    Responsibility(String),
}

/// How a run ended.
#[derive(Debug, Clone)]
pub struct RunOutcome {
    pub run_id: RunId,
    pub status: FactoryStatus,
    pub directive: Directive,
    /// Model calls actually made. Skipped cycles are not counted.
    pub assessments: u32,
    /// Cycles skipped because the observation content was unchanged.
    pub skipped: u32,
    pub counters: aftman_core::observation::Counters,
}

/// A live worker task.
struct WorkerHandle {
    id: WorkerId,
    outcome: Option<WorkerOutcome>,

    signals: mpsc::UnboundedReceiver<WorkerSignal>,
    done: mpsc::UnboundedReceiver<Result<WorkerOutcome, aftman_core::worker::WorkerError>>,
    /// The live log the worker streams into. Kept here so the runtime can copy it
    /// into the worker record before every observation — otherwise a running
    /// worker's tail would always read as empty, and the supervisor would be
    /// judging a worker it cannot see.
    output: Arc<Mutex<BoundedLog>>,
}

/// The run loop.
pub struct FactoryRuntime {
    deps: RuntimeDeps,
    builder: ObservationBuilder,
}

impl FactoryRuntime {
    #[must_use]
    pub fn new(deps: RuntimeDeps) -> Self {
        let builder = ObservationBuilder::for_worker(deps.config.worker);
        Self { deps, builder }
    }

    /// Run one job to a terminal state.
    ///
    /// # Errors
    /// Returns an error only for conditions that make the run impossible to start or
    /// continue: a missing repository, a store failure, or a worker that cannot be
    /// launched. A model failure is *not* an error here — it degrades to a
    /// conservative directive, because a supervisor that dies when its model is
    /// briefly unavailable is worse than one that says so.
    pub async fn run(&self, job: Job) -> Result<RunOutcome, RuntimeError> {
        if !job.repo.is_dir() {
            return Err(RuntimeError::BadRepo(job.repo.display().to_string()));
        }

        let run_id = RunId::new();
        let header = RunHeader {
            id: run_id,
            created_at: self.now(),
            job: job.clone(),
            config: self.deps.config.clone(),
            aftman_version: env!("CARGO_PKG_VERSION").to_owned(),
            worker_versions: BTreeMap::new(),
            granted_key_names: Vec::new(),
        };
        self.deps.store.create(&header).await?;
        let mut state = RunState::new(header, FactoryStatus::Queued);
        self.emit(
            &mut state,
            EventKind::RunCreated,
            json!({ "repo": job.repo.display().to_string() }),
        )
        .await?;

        self.transition(&mut state, FactoryStatus::Routing).await?;

        // Routing sees the job text and nothing else, and decides which *conditional*
        // responsibilities participate. It never decides what happens: a routing mistake
        // costs a dimension of supervision, not an action.
        let routing_outcome = routing::route(
            self.deps.model.as_ref(),
            &aftman_core::model::AskContext::for_run(
                Purpose::Routing,
                run_id,
                aftman_core::ids::AssessmentSeq(0),
            ),
            &self.deps.definitions,
            &job.prompt,
            &aftman_core::model::Budget::default(),
        )
        .await;

        let mut routing_payload = json!({
            "activated": routing_outcome.activated.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "degraded": routing_outcome.degraded,
            // Rounded because an f32 widened to f64 prints as `0.699999988079071`, and
            // these numbers are read off the log by people.
            "conditional": routing_outcome.decisions.iter().map(|decision| json!({
                "responsibility": decision.responsibility.to_string(),
                "probability": decision.probability.map(round4),
                "threshold": round4(decision.threshold),
                "activated": decision.activated,
            })).collect::<Vec<_>>(),
        });
        if let Some(model) = &routing_outcome.model {
            routing_payload["model"] = json!(model);
        }
        self.emit(&mut state, EventKind::JobRouted, routing_payload)
            .await?;

        // The registry is built from what routing activated, so a conditional
        // responsibility that was not activated has no checks to ask.
        let registry = ResponsibilityRegistry::from_definitions(
            &self.deps.definitions,
            &routing_outcome.activated,
            self.deps.steering_grace.as_secs(),
            1,
        )
        .map_err(RuntimeError::Responsibility)?;
        self.transition(&mut state, FactoryStatus::Running).await?;

        let mut worker: Option<WorkerHandle> = None;
        let mut last_directive: Option<Directive> = None;
        let mut assessments: u32 = 0;
        let mut skipped: u32 = 0;
        let mut next_assessment_at = self.instant();
        let mut next_idle_at = self.instant() + self.deps.config.idle_poll_interval;
        // Start with a cycle pending so the first assessment happens immediately
        // rather than after one idle interval.
        let mut activity_pending = true;
        let mut last_digest: Option<String> = None;
        let mut evidence_outcomes: Vec<EvidenceOutcome> = Vec::new();
        let mut before_finish_ran = false;

        loop {
            if let Some(finish) = self.terminal_status(&state) {
                let outcome = RunOutcome {
                    run_id,
                    status: finish,
                    directive: last_directive.unwrap_or(Directive::Continue),
                    assessments,
                    skipped,
                    counters: state.counters.clone(),
                };
                // Build the payload before taking the mutable borrow.
                let payload = json!({
                    "status": finish.as_str(),
                    "assessments": assessments,
                    "assessments_skipped": skipped,
                    "workers_run": state.counters.workers_run,
                });
                self.emit(&mut state, EventKind::RunFinished, payload)
                    .await?;
                self.snapshot(&mut state).await?;
                return Ok(outcome);
            }

            // ---- observe completions and activity without blocking -----------
            //
            // A worker completing must be observed regardless of which trigger woke
            // the loop. Draining this only inside the wait branch below is a
            // deadlock waiting to happen: if a cycle outlasts the idle interval —
            // which a real model call easily does — `idle_due` is true every
            // iteration, the wait branch is never reached, and the completion is
            // never seen. The run then supervises a worker that no longer exists.
            if let Some(handle) = worker.as_mut() {
                match handle.done.try_recv() {
                    Ok(outcome) => handle.outcome = Some(outcome?),
                    Err(
                        mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected,
                    ) => {}
                }
                while handle.signals.try_recv().is_ok() {
                    activity_pending = true;
                    next_assessment_at = self.instant() + self.deps.config.min_assessment_interval;
                }
            }

            // ---- wait for a reason to think ---------------------------------
            let idle_due = self.instant() >= next_idle_at;
            let activity_due = activity_pending && self.instant() >= next_assessment_at;
            let worker_finished = worker.as_ref().is_some_and(|w| w.outcome.is_some());
            if !(idle_due || activity_due || worker_finished) {
                let deadline = if activity_pending {
                    next_assessment_at
                } else {
                    next_idle_at
                };
                let wake = tokio::time::sleep_until(deadline);
                tokio::pin!(wake);
                match worker.as_mut() {
                    Some(handle) => {
                        tokio::select! {
                            biased;
                            Some(outcome) = handle.done.recv() => { handle.outcome = Some(outcome?); }
                            Some(_signal) = handle.signals.recv() => {
                                activity_pending = true;
                                next_assessment_at = self.instant() + self.deps.config.min_assessment_interval;
                            }
                            () = &mut wake => {}
                        }
                    }
                    None => {
                        tokio::select! {
                            () = &mut wake => {}
                        }
                    }
                }
                continue;
            }
            if idle_due {
                next_idle_at = self.instant() + self.deps.config.idle_poll_interval;
            }
            // Coalesce: this cycle consumes the pending activity.
            activity_pending = false;
            next_assessment_at = self.instant() + self.deps.config.min_assessment_interval;

            let forced = worker_finished;

            // Fold a finished worker into state before observing, and **drop the
            // handle**. Leaving a finished handle in place is a deadlock: `apply`
            // would see a worker and decline to start a new one, and
            // `worker_finished` would never become true again, so the run would
            // spin on the idle ticker forever making no progress.
            let mut just_finished = false;
            if let Some(handle) = worker.as_mut() {
                if let Some(outcome) = handle.outcome.take() {
                    self.finish_worker(&mut state, handle.id, &outcome, handle)
                        .await?;
                    state.counters.failures += u32::from(!outcome.is_success());
                    just_finished = true;
                }
            }
            if just_finished {
                worker = None;
                // Run the evidence providers that are due now, *before* the cycle that
                // judges the worker. This is what turns the agent's "3 passed" into a
                // measurement rather than a claim.
                self.collect_evidence(
                    &mut state,
                    &job,
                    RunWhen::WorkerCompleted,
                    &mut evidence_outcomes,
                )
                .await?;
            }
            // A still-running worker's live output must reach the observation.
            if let Some(handle) = worker.as_ref() {
                self.sync_output(&mut state, handle);
            }

            // ---- gather evidence --------------------------------------------
            //
            // The observation's `active` slot is the **most recent** worker, running
            // or not: the model is asked "is this done?", and it cannot answer that
            // if the worker that just finished has vanished from the evidence. Policy
            // uses a different notion — `state.active_worker()`, running only — because
            // "may I steer it" and "may I start another" are about the live process.
            let event_summaries = self.recent_event_summaries(run_id).await;
            let most_recent = state.workers.last().cloned();
            let observation = self.builder.build(
                &state,
                &job,
                most_recent.as_ref(),
                event_summaries,
                &evidence_outcomes,
                self.now(),
            );

            // ---- skip an unchanged observation ------------------------------
            let digest = observation.content_digest();
            if self.deps.config.skip_unchanged_assessments
                && !forced
                && last_digest.as_deref() == Some(digest.as_str())
                && !self.time_driven_check_due(&state)
            {
                skipped = state.counters.assessments_skipped + 1;
                state.counters.assessments_skipped = skipped;
                self.emit(
                    &mut state,
                    EventKind::AssessmentSkippedUnchanged,
                    json!({ "digest": digest }),
                )
                .await?;
                continue;
            }
            last_digest = Some(digest);

            // A judgement is formed in `Assessing`, not in `Running`. The state
            // machine encodes this deliberately: `Running -> Finished` is not a legal
            // transition, because a run must pass through the state where the
            // question was asked. `apply` returns it to `Running` afterwards.
            if state.status == FactoryStatus::Running {
                self.transition(&mut state, FactoryStatus::Assessing)
                    .await?;
            }

            // ---- ask the model ----------------------------------------------
            let questions = self.questions(&registry);
            let keys: Vec<(CheckKey, Option<f32>)> = registry
                .checks()
                .into_iter()
                .map(|c| (c.key, c.threshold))
                .collect();
            let state_json = observation.state_for(&aftman_core::observation::default_evidence());

            self.emit(
                &mut state,
                EventKind::AssessmentRequested,
                json!({ "questions": questions.keys().map(ToString::to_string).collect::<Vec<_>>() }),
            )
            .await?;

            let (checks, model_name, usage) = match self
                .deps
                .model
                .ask(
                    &aftman_core::model::AskContext::for_run(
                        Purpose::Assessment,
                        run_id,
                        aftman_core::ids::AssessmentSeq(assessments.into()),
                    ),
                    &state_json,
                    &questions,
                    &aftman_core::model::Budget::default(),
                )
                .await
            {
                Ok(answers) => {
                    state.counters.consecutive_model_failures = 0;
                    let resolved = aftman_core::model::resolve_checks(
                        &answers,
                        &keys,
                        self.deps.uncertainty_margin,
                        self.deps.ensemble_enabled,
                    );
                    (resolved, answers.model, answers.usage)
                }
                Err(error) => {
                    state.counters.consecutive_model_failures += 1;
                    // A rate limit is a different operational event from a malformed
                    // answer, and the UI shows them differently, so they are distinct
                    // event kinds rather than one `Error` with a code inside.
                    let kind = if matches!(error, ModelError::RateLimited { .. }) {
                        EventKind::RateLimited
                    } else {
                        EventKind::ModelError
                    };
                    self.emit(
                        &mut state,
                        kind,
                        json!({
                            "code": error.code(),
                            "message": error.to_string(),
                            "retryable": error.is_retryable(),
                        }),
                    )
                    .await?;
                    // No guesses: an empty check set can only produce conservative
                    // directives.
                    (BTreeMap::new(), "unavailable".to_owned(), usage_default())
                }
            };

            assessments += 1;
            state.counters.assessments = assessments;

            // ---- record the assessment --------------------------------------
            self.emit(
                &mut state,
                EventKind::AssessmentCompleted,
                json!({
                    "assessment_seq": assessments,
                    "model": model_name,
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "checks": checks.iter().map(|(k, v)| {
                        json!({
                            "key": k.to_string(),
                            "jev_p": v.jev_p,
                            "threshold": v.threshold,
                            "decisive": v.decisive,
                            "contested": v.contested,
                            "permitted": v.permitted,
                            "unresolved": v.is_unresolved(),
                        })
                    }).collect::<Vec<_>>(),
                }),
            )
            .await?;

            // ---- arbitrate ---------------------------------------------------
            let proposals = {
                let input = PolicyInput {
                    status: state.status,
                    counters: &state.counters,
                    config: &self.deps.config,
                    checks: &checks,
                    observation: &observation,
                    now: self.now(),
                    active_worker: state.active_worker(),
                    last_directive: last_directive.as_ref(),
                    ensemble_enabled: self.deps.ensemble_enabled,
                    steering_grace: self.deps.steering_grace,
                };
                registry.propose(&input)
            };

            let decision = {
                let input = PolicyInput {
                    status: state.status,
                    counters: &state.counters,
                    config: &self.deps.config,
                    checks: &checks,
                    observation: &observation,
                    now: self.now(),
                    active_worker: state.active_worker(),
                    last_directive: last_directive.as_ref(),
                    ensemble_enabled: self.deps.ensemble_enabled,
                    steering_grace: self.deps.steering_grace,
                };
                arbitrate(&input, &proposals)
            };

            // S0: the ensemble is types-and-plumbing only. Every check keeps its
            // baseline resolution, and the invariant list is still reported so the
            // log shows what the band would have done.
            self.emit(
                &mut state,
                EventKind::EnsembleResolved,
                json!({
                    "enabled": self.deps.ensemble_enabled,
                    "margin": self.deps.uncertainty_margin,
                    "unresolved": checks.iter().filter(|(_, v)| v.is_unresolved()).map(|(k, _)| k.to_string()).collect::<Vec<_>>(),
                    "rungs": Vec::<Rung>::new(),
                }),
            )
            .await?;

            for proposal in &decision.considered {
                self.emit(
                    &mut state,
                    EventKind::DirectiveProposed,
                    json!({
                        "responsibility": proposal.responsibility.to_string(),
                        "rule": proposal.rule.to_string(),
                        "directive": proposal.directive.name(),
                        "priority": proposal.priority,
                    }),
                )
                .await?;
            }
            self.emit(
                &mut state,
                EventKind::DirectiveSelected,
                json!({
                    "directive": decision.selected.name(),
                    "reason": decision.selected.reason().unwrap_or_default(),
                    "invariants": decision.invariants.iter().map(|i| format!("{i:?}")).collect::<Vec<_>>(),
                    "suppressed": decision.suppressed.iter().map(|s| json!({
                        "rule": s.rule.to_string(),
                        "reason": match &s.reason {
                            SuppressionReason::NotPermitted => "not_permitted".to_owned(),
                            SuppressionReason::Invariant(i) => format!("invariant:{i:?}"),
                            SuppressionReason::HigherPriority => "higher_priority".to_owned(),
                            SuppressionReason::CapabilityUnavailable => "capability_unavailable".to_owned(),
                        },
                    })).collect::<Vec<_>>(),
                }),
            )
            .await?;

            state.checks = checks;
            state.counters.assessments = assessments;
            state.last_directive = Some(decision.selected.clone());

            // ---- apply -------------------------------------------------------
            //
            // A `Finish` selected on evidence that has not been refreshed is exactly the
            // case `run_when = "before_finish"` exists for. Run those providers and judge
            // again, rather than finishing on a claim.
            let selected = decision.selected.clone();
            if matches!(selected, Directive::Finish { .. })
                && !before_finish_ran
                && !self.deps.evidence.due(RunWhen::BeforeFinish).is_empty()
            {
                before_finish_ran = true;
                self.collect_evidence(
                    &mut state,
                    &job,
                    RunWhen::BeforeFinish,
                    &mut evidence_outcomes,
                )
                .await?;
                state.last_directive = Some(selected.clone());
                last_directive = Some(selected);
                // The observation has changed, so the digest must not suppress the
                // re-assessment.
                last_digest = None;
                next_assessment_at = self.instant();
                activity_pending = true;
                self.snapshot(&mut state).await?;
                continue;
            }

            worker = self
                .apply(&mut state, &job, selected, worker, &observation)
                .await?;
            // A directive that neither ended the run nor parked it leaves the run
            // supervising, so return to `Running`.
            if state.status == FactoryStatus::Assessing {
                self.transition(&mut state, FactoryStatus::Running).await?;
            }
            last_directive = Some(decision.selected.clone());
            self.snapshot(&mut state).await?;

            if self.timeout_exceeded(&state) {
                self.transition(&mut state, FactoryStatus::Failed).await?;
            }
        }
    }

    // ---- helpers ----------------------------------------------------------

    fn now(&self) -> OffsetDateTime {
        self.deps.clock.now()
    }

    fn instant(&self) -> tokio::time::Instant {
        self.deps.clock.instant()
    }

    fn timeout_exceeded(&self, state: &RunState) -> bool {
        let elapsed = (self.now() - state.header.created_at).unsigned_abs();
        elapsed >= self.deps.config.overall_timeout
    }

    /// Whether a time-based decision will trip within the next tick.
    ///
    /// The content digest deliberately ignores elapsed time, so a timeout
    /// approaching is invisible to it. Without this check the digest skip could
    /// suppress the one cycle that would notice the timeout.
    fn time_driven_check_due(&self, state: &RunState) -> bool {
        let elapsed = (self.now() - state.header.created_at).unsigned_abs();
        let tick = self.deps.config.idle_poll_interval;
        let remaining = self.deps.config.overall_timeout.saturating_sub(elapsed);
        remaining <= tick
    }

    /// A terminal status, or `None` to keep supervising.
    ///
    /// `NeedsHuman` ends an M0 run: there is no answer channel yet, so the run stops
    /// with the question recorded rather than proceeding on a guess.
    fn terminal_status(&self, state: &RunState) -> Option<FactoryStatus> {
        if state.status.is_terminal() {
            return Some(state.status);
        }
        if state.status == FactoryStatus::NeedsHuman {
            return Some(FactoryStatus::NeedsHuman);
        }
        None
    }

    fn questions(&self, registry: &ResponsibilityRegistry) -> BTreeMap<CheckKey, Question> {
        registry
            .checks()
            .into_iter()
            .map(|c| (c.key, c.question))
            .collect()
    }

    async fn transition(
        &self,
        state: &mut RunState,
        to: FactoryStatus,
    ) -> Result<(), RuntimeError> {
        let from = state.status;
        state.status = from.transition(to)?;
        self.emit(
            state,
            EventKind::StateChanged,
            json!({ "from": from.as_str(), "to": to.as_str() }),
        )
        .await
        .map(|_| ())
    }

    async fn snapshot(&self, state: &mut RunState) -> Result<(), RuntimeError> {
        self.deps.store.snapshot(state).await?;
        Ok(())
    }

    async fn emit(
        &self,
        state: &mut RunState,
        kind: EventKind,
        payload: serde_json::Value,
    ) -> Result<EventEnvelope, RuntimeError> {
        let seq = state.last_seq.next();
        let event = EventEnvelope::new(seq, state.header.id, self.now(), kind, payload);
        self.deps.store.append(&event).await?;
        state.last_seq = seq;
        self.deps.sink.emit(&event);
        Ok(event)
    }

    async fn recent_event_summaries(&self, run_id: RunId) -> Vec<EventSummary> {
        let from = EventSeq(1);
        match self.deps.store.events(run_id, from).await {
            Ok(events) => events
                .into_iter()
                .map(|e| EventSummary {
                    seq: e.seq,
                    kind: e.kind.as_str().to_owned(),
                    summary: e.payload["summary"]
                        .as_str()
                        .map_or_else(|| e.summary(), ToOwned::to_owned),
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Copy a live worker's streamed output into its record.
    ///
    /// The worker writes into a shared bounded log; the record is what the
    /// observation reads. Without this sync, a running worker's tail would always be
    /// empty.
    fn sync_output(&self, state: &mut RunState, handle: &WorkerHandle) {
        let snapshot = handle
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(record) = state.workers.iter_mut().find(|w| w.id == handle.id) {
            record.output = snapshot;
        }
    }

    async fn finish_worker(
        &self,
        state: &mut RunState,
        id: WorkerId,
        outcome: &WorkerOutcome,
        handle: &WorkerHandle,
    ) -> Result<(), RuntimeError> {
        let ended_at = self.now();
        let snapshot = handle
            .output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(record) = state.workers.iter_mut().find(|w| w.id == id) {
            record.status = outcome.status();
            record.exit = outcome.exit();
            record.ended_at = Some(ended_at);
            // The final output must survive the worker's exit; otherwise the last
            // thing a worker did before finishing is invisible.
            record.output = snapshot;
        }
        self.emit(
            state,
            EventKind::WorkerCompleted,
            json!({
                "worker_id": id.to_string(),
                "outcome": format!("{outcome:?}"),
                "success": outcome.is_success(),
            }),
        )
        .await?;
        Ok(())
    }

    /// Run the providers due for one trigger, recording each outcome.
    ///
    /// Every provider produces an event whether it succeeded, failed, or could not be
    /// launched: a check that expects a measurement and silently gets nothing is how a
    /// run finishes on a claim.
    async fn collect_evidence(
        &self,
        state: &mut RunState,
        job: &Job,
        trigger: RunWhen,
        outcomes: &mut Vec<EvidenceOutcome>,
    ) -> Result<(), RuntimeError> {
        let due: Vec<aftman_core::evidence::CommandProvider> = self
            .deps
            .evidence
            .due(trigger)
            .into_iter()
            .cloned()
            .collect();
        for provider in due {
            let outcome = aftman_workers::run_provider(
                &provider,
                &job.repo,
                &self.deps.evidence_temp_dir,
                self.now(),
            )
            .await;
            let payload = json!({
                "provider": outcome.provider.to_string(),
                "argv": outcome.argv,
                "exit_code": outcome.exit_code,
                "success": outcome.success,
                "duration_ms": outcome.duration.as_millis() as u64,
                "truncated": outcome.truncated,
                "launch_error": outcome.launch_error,
                "summary": outcome.summary(),
            });
            outcomes.push(outcome);
            self.emit(state, EventKind::EvidenceCollected, payload)
                .await?;
        }
        Ok(())
    }

    /// Spawn a worker as an independent task.
    #[allow(clippy::too_many_arguments)]
    async fn start_worker(
        &self,
        state: &mut RunState,
        job: &Job,
        task: TaskKind,
        mission: String,
        parent: Option<WorkerId>,
    ) -> Result<WorkerHandle, RuntimeError> {
        let id = WorkerId::new();
        let capacity = self.deps.config.obs_tail_chars.max(4_096);
        let output = Arc::new(Mutex::new(BoundedLog::new(capacity, capacity)));

        let mut record = WorkerRecord::new(
            id,
            self.deps.config.worker,
            task,
            mission.clone(),
            BoundedLog::new(capacity, capacity),
            self.deps.worker.supports_steering(),
        );
        record.parent = parent;
        record.status = WorkerStatus::Running;
        record.started_at = Some(self.now());
        record.artifact_ref = self.deps.worker.artifact_ref();
        state.workers.push(record);
        state.counters.workers_run += 1;
        if task == TaskKind::Code {
            state.counters.attempts += 1;
        } else {
            state.counters.verifiers_run += 1;
        }

        self.emit(
            state,
            EventKind::WorkerStarted,
            json!({
                "worker_id": id.to_string(),
                "kind": self.deps.config.worker.as_str(),
                "task": format!("{task:?}"),
                "mission_excerpt": mission.chars().take(200).collect::<String>(),
                "supports_steering": self.deps.worker.supports_steering(),
            }),
        )
        .await?;

        let (signal_tx, signal_rx) = mpsc::unbounded_channel();
        let (done_tx, done_rx) = mpsc::unbounded_channel();
        let sink = WorkerEventSink::new(signal_tx);

        let ctx = WorkerCtx {
            id,
            kind: self.deps.config.worker,
            task,
            repo: job.repo.clone(),
            mission,
            model: self.deps.config.worker_model.clone(),
            timeout: self.deps.config.worker_timeout,
            env: Vec::new(),
            output: Arc::clone(&output),
        };

        let worker = Arc::clone(&self.deps.worker);
        tokio::spawn(async move {
            let outcome = worker.run(ctx, sink).await;
            let _ = done_tx.send(outcome);
        });

        Ok(WorkerHandle {
            id,
            outcome: None,
            signals: signal_rx,
            done: done_rx,
            output,
        })
    }

    /// Apply one directive, returning the (possibly new) worker handle.
    async fn apply(
        &self,
        state: &mut RunState,
        job: &Job,
        directive: Directive,
        worker: Option<WorkerHandle>,
        observation: &Observation,
    ) -> Result<Option<WorkerHandle>, RuntimeError> {
        match directive {
            Directive::Continue => Ok(worker),

            Directive::StartWorker { .. } => {
                if worker.is_some() {
                    return Ok(worker);
                }
                let mission = code_mission(job, observation);
                Ok(Some(
                    self.start_worker(state, job, TaskKind::Code, mission, None)
                        .await?,
                ))
            }

            Directive::Retry { .. } => {
                // A retry supersedes whatever was running.
                if worker.is_some() {
                    self.deps
                        .worker
                        .terminate(TerminateReason::Superseded)
                        .await?;
                    self.emit(
                        state,
                        EventKind::WorkerTerminated,
                        json!({ "reason": "superseded" }),
                    )
                    .await?;
                }
                let mission = code_mission(job, observation);
                Ok(Some(
                    self.start_worker(state, job, TaskKind::Code, mission, None)
                        .await?,
                ))
            }

            Directive::Verify { .. } => {
                let mission = verify_mission(job);
                Ok(Some(
                    self.start_worker(state, job, TaskKind::Verify, mission, None)
                        .await?,
                ))
            }

            Directive::Steer { reason, .. } => {
                let Some(handle) = worker.as_ref() else {
                    return Ok(worker);
                };
                let message = reason.clone();
                let outcome = self.deps.worker.steer(&message).await?;
                if let Some(record) = state.workers.iter_mut().find(|w| w.id == handle.id) {
                    record.last_steered_at = Some(self.now());
                    record.steers.push(SteerRecord {
                        at: self.now(),
                        message_excerpt: message.chars().take(200).collect(),
                        outcome: outcome.clone(),
                        queue_depth: None,
                    });
                }
                if outcome.is_accepted() {
                    state.counters.steers_sent += 1;
                }
                self.emit(
                    state,
                    EventKind::WorkerSteered,
                    json!({ "outcome": format!("{outcome:?}") }),
                )
                .await?;
                Ok(worker)
            }

            Directive::Stop { ref reason } => {
                self.deps.worker.terminate(TerminateReason::Stopped).await?;
                let ended_at = self.now();
                if let Some(handle) = worker.as_ref() {
                    if let Some(record) = state.workers.iter_mut().find(|w| w.id == handle.id) {
                        record.status = WorkerStatus::Stopped;
                        record.exit = Some(aftman_core::job::WorkerExit::terminated());
                        record.ended_at = Some(ended_at);
                    }
                }
                self.emit(
                    state,
                    EventKind::WorkerTerminated,
                    json!({ "reason": reason, "cause": "stopped" }),
                )
                .await?;
                self.transition(state, FactoryStatus::Stopped).await?;
                // The handle is dropped: the worker is stopped, and a later `Retry`
                // starts a fresh one.
                Ok(None)
            }

            Directive::Finish { .. } => {
                self.transition(state, FactoryStatus::Finished).await?;
                Ok(worker)
            }

            Directive::Escalate { reason, ask } => {
                let escalation = Escalation {
                    id: EscalationId::new(),
                    kind: escalation_kind(&reason),
                    raised_at: self.now(),
                    check: None,
                    ask: ask.clone(),
                    jev_p: None,
                    opinions: Vec::new(),
                    answered: None,
                };
                state.escalations.push(escalation);
                self.emit(
                    state,
                    EventKind::EscalationRaised,
                    json!({ "reason": reason, "ask": ask }),
                )
                .await?;
                self.transition(state, FactoryStatus::NeedsHuman).await?;
                Ok(worker)
            }
        }
    }
}

/// Round a probability to four decimals, for log readability.
fn round4(value: f32) -> f64 {
    (f64::from(value) * 10_000.0).round() / 10_000.0
}

fn escalation_kind(reason: &str) -> EscalationKind {
    if reason.contains("model_unavailable") {
        EscalationKind::ModelUnavailable
    } else if reason.contains("budget") {
        EscalationKind::AssessmentBudget
    } else {
        EscalationKind::Check
    }
}

fn usage_default() -> aftman_core::model::Usage {
    aftman_core::model::Usage::default()
}

/// The mission sent to a coding worker.
///
/// Assembled deterministically and stored verbatim, so a decision can always be
/// traced to the exact text the worker received.
#[must_use]
pub fn code_mission(job: &Job, observation: &Observation) -> String {
    let tests = if observation.tests.is_empty() {
        String::new()
    } else {
        format!(
            "\nPrevious test evidence (a claim from earlier output, not verified by the supervisor): {}\n",
            observation
                .tests
                .iter()
                .map(|t| t.raw.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        )
    };
    format!(
        "You are working in the repository at {}.\n\
         All commands run there. Do not ask for confirmation; complete the work and run the tests.\n\n\
         ## Job\n{}\n{}\n\
         ## Worker contract\n\
         You are being supervised. Report progress through normal tool use and output.\n\
         Do not claim a test passed unless you ran it in this session.\n",
        job.repo.display(),
        job.prompt,
        tests
    )
}

/// The mission sent to an independent verifier.
///
/// The verifier is told explicitly not to trust the coding agent's reports, and not
/// to modify anything. A verifier that mutates the repository makes its own result
/// unusable.
#[must_use]
pub fn verify_mission(job: &Job) -> String {
    format!(
        "You are an independent verifier. You did not write this code and must not trust\n\
         the previous agent's reports.\n\n\
         Repository: {}\n\
         Original job: {}\n\n\
         1. Inspect the working tree and the change (git status, git diff).\n\
         2. Determine what the job actually required.\n\
         3. Run the repository's own checks. Record the exact commands and their results.\n\
         4. Try to falsify the claim that the job is complete.\n\
         5. Report PASS or FAIL, the exact evidence, and any requirement you could not evaluate.\n\n\
         Do not modify the repository. Do not fix anything. Report only.\n",
        job.repo.display(),
        job.prompt
    )
}

/// Whether a resolved check is permitted, for a caller that does not want to reach
/// into the whole record.
#[must_use]
pub fn is_permitted(check: &ResolvedCheck) -> bool {
    check.permitted
}

/// The invariants a decision reported, for a surface.
#[must_use]
pub fn reported_invariants(invariants: &[Invariant]) -> Vec<String> {
    invariants.iter().map(|i| format!("{i:?}")).collect()
}

/// A proposal list rendered for a log line.
#[must_use]
pub fn render_proposals(proposals: &[Proposal]) -> Vec<String> {
    proposals
        .iter()
        .map(|p| format!("{}:{} @{}", p.responsibility, p.rule, p.priority))
        .collect()
}

/// Steering outcomes that count against a worker's steer budget.
#[must_use]
pub fn steer_was_accepted(outcome: &SteerOutcome) -> bool {
    outcome.is_accepted()
}

/// Convenience for building a runtime with a simulation-friendly default set.
#[must_use]
pub fn default_margin() -> f32 {
    0.15
}

#[cfg(test)]
mod tests {
    use super::{code_mission, verify_mission};
    use aftman_core::job::{Job, JobSource, JobSummary};
    use aftman_core::observation::{
        Counters, GitEvidence, Observation, TestResult, TestSource, TestStatus,
    };
    use std::path::PathBuf;

    fn job() -> Job {
        Job {
            prompt: "Add request retries with backoff".into(),
            repo: PathBuf::from("/repo"),
            source: JobSource::Cli,
        }
    }

    fn observation() -> Observation {
        Observation {
            run_id: aftman_core::ids::RunId::new(),
            status: aftman_core::job::FactoryStatus::Assessing,
            job: JobSummary {
                prompt_excerpt: "add retries".into(),
                prompt_truncated: false,
                repo: PathBuf::from("/repo"),
                elapsed_secs: 0,
            },
            active: None,
            history: Vec::new(),
            git: GitEvidence::default(),
            untracked: Vec::new(),
            instructions: None,
            tests: Vec::new(),
            events: Vec::new(),
            prior: None,
            counters: Counters::default(),
            evidence: Default::default(),
        }
    }

    #[test]
    fn the_code_mission_states_the_repo_and_the_job() {
        let mission = code_mission(&job(), &observation());
        assert!(mission.contains("/repo"));
        assert!(mission.contains("Add request retries with backoff"));
        assert!(
            mission.contains("Do not claim a test passed unless you ran it"),
            "the worker contract must be explicit"
        );
    }

    #[test]
    fn previous_test_evidence_is_labelled_as_a_claim() {
        let mut obs = observation();
        obs.tests.push(TestResult {
            source: TestSource::WorkerOutput,
            passed: 3,
            failed: 0,
            errored: 0,
            skipped: 0,
            status: TestStatus::Passed,
            raw: "3 passed".into(),
        });
        let mission = code_mission(&job(), &obs);
        assert!(mission.contains("3 passed"));
        assert!(
            mission.contains("not verified by the supervisor"),
            "a claim must never be presented as a fact"
        );
    }

    #[test]
    fn the_verify_mission_forbids_mutation_and_trust() {
        let mission = verify_mission(&job());
        assert!(mission.contains("must not trust"));
        assert!(mission.contains("Do not modify the repository"));
        assert!(mission.contains("Report only"));
    }
}
