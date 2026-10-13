//! The control plane of a run: how something outside the run loop steers, cancels,
//! or answers it while it is running.
//!
//! A [`RunControl`] is a cheap, cloneable handle around a bounded channel. Every
//! method sends one message and **awaits a reply** saying what actually happened, so
//! a caller never has to guess whether a cancel took effect. The run loop is the only
//! consumer ([`ControlReceiver`]); it observes messages at the top of each iteration
//! and in its wait `select!`, never in the middle of a cycle (`docs/02-lld.md` §7.1),
//! and replies only after the effect has been applied and persisted.
//!
//! Nothing here decides anything. A control message is a request that the runtime
//! validates against the run's state; the only directive-shaped effect a human can
//! cause is *stopping* work (`docs/06-ensemble.md` R3).

use tokio::sync::{mpsc, oneshot};

use aftman_core::assessment::HumanDecision;
use aftman_core::ids::EscalationId;
use aftman_core::worker::SteerOutcome;

/// Pending control messages a run will queue before a sender has to wait.
pub const CONTROL_QUEUE_DEPTH: usize = 16;

/// Why a control request did not take effect.
///
/// The protocol layer maps these onto the documented error codes (`docs/02-lld.md`
/// §7.1): `WrongState` to `wrong_state`, `Internal` to `internal`, `RunEnded` and
/// `UnknownEscalation` to `unknown_run` or `wrong_state` as the method dictates.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlError {
    /// The run has already ended, or its control channel is closed, so nothing can
    /// act on the request. Covers a send to a finished run and a run that finished
    /// before it replied.
    #[error("the run has ended")]
    RunEnded,

    /// The run is alive but not in a state where this request means anything, for
    /// example steering with no active worker. The message is fit to show a person.
    #[error("wrong state: {0}")]
    WrongState(String),

    /// The id names no escalation of this run, or one that was already answered.
    #[error("escalation {0} is unknown or was already answered")]
    UnknownEscalation(EscalationId),

    /// The request itself is malformed, for example an empty steer message.
    #[error("invalid request: {0}")]
    Invalid(String),

    /// The runtime failed while applying the request (storage, worker I/O).
    #[error("internal error: {0}")]
    Internal(String),
}

/// A request to the run loop, with its reply slot.
#[derive(Debug)]
pub(crate) enum ControlMessage {
    Steer {
        message: String,
        reply: oneshot::Sender<Result<SteerOutcome, ControlError>>,
    },
    Cancel {
        reason: String,
        reply: oneshot::Sender<Result<(), ControlError>>,
    },
    Answer {
        escalation_id: EscalationId,
        decision: HumanDecision,
        note: String,
        surface: String,
        reply: oneshot::Sender<Result<(), ControlError>>,
    },
}

/// The sending half. Clone freely; the run treats its control channel as closed once
/// every clone is dropped.
#[derive(Debug, Clone)]
pub struct RunControl {
    tx: mpsc::Sender<ControlMessage>,
}

/// The receiving half, owned by the run loop (`FactoryRuntime::drive`).
#[derive(Debug)]
pub struct ControlReceiver {
    rx: mpsc::Receiver<ControlMessage>,
}

impl RunControl {
    /// A fresh control channel for one run.
    #[must_use]
    pub fn channel() -> (Self, ControlReceiver) {
        let (tx, rx) = mpsc::channel(CONTROL_QUEUE_DEPTH);
        (Self { tx }, ControlReceiver { rx })
    }

    /// Deliver guidance to the active worker as a human steer.
    ///
    /// A backend that refuses (`Rejected`) or cannot accept input (`Unsupported`) is
    /// an `Ok` outcome, not an error: the attempt happened and was recorded.
    ///
    /// # Errors
    /// [`ControlError::WrongState`] when there is no active worker.
    pub async fn steer(&self, message: impl Into<String>) -> Result<SteerOutcome, ControlError> {
        let (reply, rx) = oneshot::channel();
        self.request(
            ControlMessage::Steer {
                message: message.into(),
                reply,
            },
            rx,
        )
        .await
    }

    /// End the run as `Cancelled`, terminating any active worker.
    ///
    /// # Errors
    /// [`ControlError::RunEnded`] if the run already finished.
    pub async fn cancel(&self, reason: impl Into<String>) -> Result<(), ControlError> {
        let (reply, rx) = oneshot::channel();
        self.request(
            ControlMessage::Cancel {
                reason: reason.into(),
                reply,
            },
            rx,
        )
        .await
    }

    /// Answer an open escalation.
    ///
    /// # Errors
    /// [`ControlError::UnknownEscalation`] when the id is not an open escalation of
    /// this run, [`ControlError::WrongState`] when the run is not waiting.
    pub async fn answer(
        &self,
        escalation_id: EscalationId,
        decision: HumanDecision,
        note: impl Into<String>,
        surface: impl Into<String>,
    ) -> Result<(), ControlError> {
        let (reply, rx) = oneshot::channel();
        self.request(
            ControlMessage::Answer {
                escalation_id,
                decision,
                note: note.into(),
                surface: surface.into(),
                reply,
            },
            rx,
        )
        .await
    }

    /// Whether the run loop is still listening.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    async fn request<T>(
        &self,
        message: ControlMessage,
        reply: oneshot::Receiver<Result<T, ControlError>>,
    ) -> Result<T, ControlError> {
        self.tx
            .send(message)
            .await
            .map_err(|_| ControlError::RunEnded)?;
        // A dropped reply slot means the loop ended with this message still queued.
        reply.await.map_err(|_| ControlError::RunEnded)?
    }
}

impl ControlReceiver {
    /// Wait for the next message. `None` once every [`RunControl`] is dropped and the
    /// queue is empty.
    pub(crate) async fn recv(&mut self) -> Option<ControlMessage> {
        self.rx.recv().await
    }

    /// The next queued message without waiting.
    pub(crate) fn try_recv(&mut self) -> Result<ControlMessage, mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
}

#[cfg(test)]
mod tests {
    use super::{ControlError, RunControl};

    #[tokio::test]
    async fn a_request_to_a_dropped_receiver_is_run_ended() {
        let (control, receiver) = RunControl::channel();
        drop(receiver);
        assert!(control.is_closed());
        assert_eq!(control.cancel("x").await, Err(ControlError::RunEnded));
        assert_eq!(control.steer("x").await, Err(ControlError::RunEnded));
    }

    #[tokio::test]
    async fn a_queued_request_is_run_ended_when_the_loop_drops_it() {
        let (control, receiver) = RunControl::channel();
        let pending = tokio::spawn({
            let control = control.clone();
            async move { control.cancel("x").await }
        });
        tokio::task::yield_now().await;
        // The loop ends without ever reading the message.
        drop(receiver);
        assert_eq!(pending.await.ok(), Some(Err(ControlError::RunEnded)));
    }
}
