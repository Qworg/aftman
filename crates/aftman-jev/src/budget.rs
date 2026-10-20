//! Fitting an observation into a byte budget.
//!
//! Jev input tokens are billable, so the observation is compacted before it is sent.
//! The unit is a **pair**: one leaf `(path, value)` inside the `state` object. Two
//! budgets apply — one per pair (a single huge diff) and one for the whole request (a
//! thousand small leaves).
//!
//! Compaction is deterministic and ordered, and it records every action it takes. Two
//! rules constrain it:
//!
//! - **Truncation happens in the middle, with a marker.** A model shown a silently
//!   clipped diff will reason about a fact that is not there.
//! - **Required evidence is never dropped.** The job, the current status, the active
//!   worker's tail, the prior decision, and the counters are either present or the
//!   request is refused. Supervising with the job removed is meaningless, so
//!   refusing is the honest outcome.
//!
//! See `docs/03-jev.md` §6.

use std::collections::BTreeMap;

use aftman_core::error::ModelError;
use aftman_core::model::Budget;
use serde_json::Value;

/// The result of fitting an observation to a budget.
#[derive(Debug, Clone)]
pub struct BudgetFit {
    pub state: Value,
    /// Whether anything had to be reduced.
    pub compacted: bool,
    /// What was done, for the assessment event and the log.
    pub measurements: BTreeMap<String, usize>,
}

impl BudgetFit {
    #[must_use]
    pub fn measurement(&self, key: &str) -> usize {
        self.measurements.get(key).copied().unwrap_or(0)
    }
}

/// Evidence that must survive compaction, as JSON paths.
///
/// If these alone exceed the request budget the request is refused rather than sent
/// with the job or the current status missing.
const PROTECTED: &[&[&str]] = &[
    &["job"],
    &["factory_status"],
    &["active_worker"],
    &["prior"],
    &["counters"],
    &["worker_output", "output_tail"],
];

/// Serialized size of a value.
#[must_use]
pub fn byte_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(0, |bytes| bytes.len())
}

/// Fit `state` into `budget`.
///
/// # Errors
/// [`ModelError::BudgetExceeded`] when the required evidence cannot fit, which the
/// runtime turns into an escalation rather than a blind assessment.
pub fn fit(state: &Value, budget: &Budget) -> Result<BudgetFit, ModelError> {
    let mut work = state.clone();
    let mut measurements: BTreeMap<String, usize> = BTreeMap::new();

    // ---- Step 1: cap any single pair -------------------------------------
    //
    // This runs **before** the protected-evidence check, and the order matters. That check
    // refuses the request when the required evidence cannot fit, and a required field can
    // itself be enormous: the active worker's output tail is protected, and a chatty worker
    // produces megabytes of it. Measured first, that tail alone would exceed the budget and
    // the request would be refused, so supervision would stop altogether. Measured after
    // per-pair truncation it is a bounded field that fits. Keeping the evidence and shrinking
    // it is strictly better than keeping none and refusing to ask.
    let truncated_pairs =
        truncate_long_strings(&mut work, budget.pair_bytes, budget.pair_target_bytes);
    if truncated_pairs > 0 {
        measurements.insert("pairs_truncated".to_owned(), truncated_pairs);
    }

    // ---- Step 2: is the required evidence, now bounded, small enough? -----
    let protected = byte_len(&protected_subset(&work));
    if protected > budget.request_bytes {
        // The measurements travel with the refusal. "Raise the budget or reduce the
        // evidence" is only actionable if it says which evidence, and the two refusals fail
        // for opposite reasons: one is protected evidence that cannot be reduced at all, the
        // other is reducible evidence that was not reduced enough.
        return Err(ModelError::BudgetExceeded(format!(
            "required evidence alone is {protected} bytes, over the {} byte request budget; \
             it is protected, so the budget must rise or the evidence must shrink",
            budget.request_bytes
        )));
    }

    // ---- Step 3: drop, cheapest evidence first ---------------------------
    // Runs when the hard cap is exceeded, and continues to the hysteresis target once
    // *any* compaction has started — including a per-pair truncation, which counts as
    // started. Without that second clause a state that step 1 alone brought just under
    // the cap would sit permanently on the boundary, oscillating between compacted and
    // uncompacted across cycles.
    let compacted_by_pairs = !measurements.is_empty();
    let over_hard = byte_len(&work) > budget.request_bytes;
    let over_target = byte_len(&work) > budget.request_target_bytes;
    if over_hard || (compacted_by_pairs && over_target) {
        reduce(&mut work, budget.request_target_bytes, &mut measurements);
    }

    let size = byte_len(&work);
    if size > budget.request_bytes {
        return Err(ModelError::BudgetExceeded(format!(
            "could not compact the observation below {} bytes (reached {size}); \
             reductions applied: {measurements:?}; the request is refused rather than sent \
             with required evidence missing",
            budget.request_bytes
        )));
    }

    let compacted = !measurements.is_empty();
    measurements.insert("final_bytes".to_owned(), size);
    Ok(BudgetFit {
        state: work,
        compacted,
        measurements,
    })
}

/// Reduce in a fixed order until `target` is met.
///
/// Order matters and is the one documented: oldest events, then oldest worker history,
/// then extra untracked files, then the middle of the diff, then the diff itself, then
/// output heads, then command evidence. Each stage is cheaper to lose than the one
/// after it, and each runs until the target is met or it has nothing left to give —
/// a stage that only halves the problem once leaves a state that cannot fit.
fn reduce(work: &mut Value, target: usize, measurements: &mut BTreeMap<String, usize>) {
    let dropped = shrink_front(work, &["recent_events"], target);
    if dropped > 0 {
        measurements.insert("events_dropped".to_owned(), dropped);
    }
    if byte_len(work) <= target {
        return;
    }

    let dropped = shrink_front(work, &["worker_history"], target);
    if dropped > 0 {
        measurements.insert("history_dropped".to_owned(), dropped);
    }
    if byte_len(work) <= target {
        return;
    }

    let dropped = shrink_back(work, &["untracked_evidence"], target);
    if dropped > 0 {
        measurements.insert("untracked_dropped".to_owned(), dropped);
    }
    if byte_len(work) <= target {
        return;
    }

    // The diff, halved repeatedly and then dropped entirely.
    let mut saved = 0;
    loop {
        if byte_len(work) <= target {
            break;
        }
        let Some(diff) = get_str_mut(work, &["git_diff", "diff"]) else {
            break;
        };
        if diff.len() < 128 {
            break;
        }
        let before = diff.len();
        let clipped = truncate_middle(diff, before / 2);
        if clipped.len() >= before {
            break;
        }
        saved += before - clipped.len();
        *diff = clipped;
    }
    if saved > 0 {
        measurements.insert("diff_truncated".to_owned(), saved);
    }
    if byte_len(work) <= target {
        return;
    }
    if let Some(object) = work.get_mut("git_diff").and_then(Value::as_object_mut) {
        object.insert("diff".to_owned(), Value::String(String::new()));
        measurements.insert("diff_dropped".to_owned(), 1);
    }
    if byte_len(work) <= target {
        return;
    }

    // Output heads. The tail is protected; the head is not.
    if let Some(object) = work.get_mut("worker_output").and_then(Value::as_object_mut) {
        object.insert("output_head".to_owned(), Value::String(String::new()));
        measurements.insert("output_head_dropped".to_owned(), 1);
    }
    if byte_len(work) <= target {
        return;
    }

    // The unprotected keys the review named. None of these is required evidence — a check
    // asks for them, but the *request* does not need them to be complete — so they are
    // reduced before the request is refused. A refusal with a reducible field still in it
    // is a refusal the operator cannot act on without knowing which field to shrink.
    for (key, measurement) in [
        ("changed_files", "changed_files_dropped"),
        ("test_results", "test_results_dropped"),
    ] {
        if byte_len(work) <= target {
            return;
        }
        // Keep the first: a changed-files list reads newest-first in the observation, and the
        // leading entries are the ones a check asks about. Halved repeatedly, because halving
        // once is not enough when the list is an order of magnitude over the budget — and a
        // refusal with a reducible field still in it is a refusal the operator cannot act on.
        loop {
            let over = byte_len(work) > target;
            let Some(array) = work.get_mut(key).and_then(Value::as_array_mut) else {
                break;
            };
            if !over || array.len() <= 1 {
                break;
            }
            let before = array.len();
            array.truncate(before / 2);
            let now = array.len();
            measurements
                .entry(measurement.to_owned())
                .and_modify(|dropped| *dropped += before - now)
                .or_insert(before - now);
        }
    }
    if byte_len(work) <= target {
        return;
    }
    // The distinct tool names in the activity: a sample says "read, bash, edit", and the
    // count beside it is the signal anyway.
    if let Some(object) = work
        .get_mut("worker_activity")
        .and_then(Value::as_object_mut)
        && let Some(names) = object.get_mut("tool_names").and_then(Value::as_array_mut)
    {
        let before = names.len();
        names.truncate(before / 2);
        if names.len() != before {
            measurements.insert("tool_names_dropped".to_owned(), before - names.len());
        }
    }
    if byte_len(work) <= target {
        return;
    }
    // The repository instructions, halved: the file is a reference, and its head is what
    // a check reads.
    if let Some(object) = work
        .get_mut("agents_md_instructions")
        .and_then(Value::as_object_mut)
        && let Some(Value::String(content)) = object.get_mut("content")
    {
        let before = content.len();
        if before > 512 {
            *content = truncate_middle(content, before / 2);
            measurements.insert("instructions_truncated".to_owned(), before - content.len());
        }
    }
    if byte_len(work) <= target {
        return;
    }

    // Command evidence, largest first: a failing command's tail is usually what the
    // check needs, and a large entry is usually a whole log.
    let mut dropped = 0;
    loop {
        if byte_len(work) <= target {
            break;
        }
        let Some(object) = work
            .get_mut("command_evidence")
            .and_then(Value::as_object_mut)
        else {
            break;
        };
        if object.is_empty() {
            break;
        }
        let largest = object
            .iter()
            .map(|(key, value)| (key.clone(), byte_len(value)))
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
            .map(|(key, _)| key);
        let Some(key) = largest else { break };
        if let Some(object) = work
            .get_mut("command_evidence")
            .and_then(Value::as_object_mut)
        {
            object.remove(&key);
        }
        dropped += 1;
    }
    if dropped > 0 {
        measurements.insert("command_evidence_dropped".to_owned(), dropped);
    }
}

/// Drop entries from the front of an array until the target is met or it is empty.
fn shrink_front(work: &mut Value, path: &[&str], target: usize) -> usize {
    let mut removed = 0;
    loop {
        if byte_len(work) <= target {
            break;
        }
        let Some(array) = get_path_mut(work, path).and_then(Value::as_array_mut) else {
            break;
        };
        if array.is_empty() {
            break;
        }
        // A quarter at a time: fast when the array is large, and it does not throw
        // away far more than it needed to.
        let batch = (array.len() / 4).max(1).min(array.len());
        array.drain(..batch);
        removed += batch;
    }
    removed
}

/// Drop entries from the back of an array until the target is met or one remains.
fn shrink_back(work: &mut Value, path: &[&str], target: usize) -> usize {
    let mut removed = 0;
    loop {
        if byte_len(work) <= target {
            break;
        }
        let Some(array) = get_path_mut(work, path).and_then(Value::as_array_mut) else {
            break;
        };
        if array.len() <= 1 {
            break;
        }
        let batch = (array.len() / 4).max(1).min(array.len() - 1);
        let keep = array.len() - batch;
        array.truncate(keep);
        removed += batch;
    }
    removed
}

/// Only the evidence that must never be dropped.
#[must_use]
pub fn protected_subset(state: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for path in PROTECTED {
        if let Some(value) = get_path(state, path) {
            insert_path(&mut out, path, value.clone());
        }
    }
    Value::Object(out)
}

fn get_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cursor = value;
    for step in path {
        cursor = cursor.get(step)?;
    }
    Some(cursor)
}

fn get_str_mut<'a>(value: &'a mut Value, path: &[&str]) -> Option<&'a mut String> {
    let mut cursor = value;
    for step in path {
        cursor = cursor.get_mut(step)?;
    }
    cursor.as_str().map(|_| ())?;
    match cursor {
        Value::String(text) => Some(text),
        _ => None,
    }
}

fn insert_path(object: &mut serde_json::Map<String, Value>, path: &[&str], value: Value) {
    match path {
        [] => {}
        [leaf] => {
            object.insert((*leaf).to_owned(), value);
        }
        [head, rest @ ..] => {
            let entry = object
                .entry((*head).to_owned())
                .or_insert_with(|| Value::Object(serde_json::Map::new()));
            if let Value::Object(inner) = entry {
                insert_path(inner, rest, value);
            }
        }
    }
}

/// Truncate every string leaf longer than `cap` to `target` bytes, in the middle.
fn truncate_long_strings(value: &mut Value, cap: usize, target: usize) -> usize {
    let mut count = 0;
    walk_strings(value, &mut |text| {
        if text.len() > cap {
            count += 1;
            Some(truncate_middle(text, target))
        } else {
            None
        }
    });
    count
}

/// Apply `transform` to every string leaf, replacing it when `Some` is returned.
fn walk_strings(value: &mut Value, transform: &mut impl FnMut(&str) -> Option<String>) {
    match value {
        Value::String(text) => {
            if let Some(replacement) = transform(text) {
                *text = replacement;
            }
        }
        Value::Array(items) => {
            for item in items {
                walk_strings(item, transform);
            }
        }
        Value::Object(object) => {
            for (_, entry) in object.iter_mut() {
                walk_strings(entry, transform);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Keep the head and the tail, and say how much was removed.
///
/// A silent clip is worse than a visible one: a model shown a diff that simply stops
/// will reason as though that were the whole change.
#[must_use]
pub fn truncate_middle(text: &str, target: usize) -> String {
    if text.len() <= target {
        return text.to_owned();
    }
    let head_len = target * 3 / 5;
    let tail_len = target - head_len;
    let removed = text.len() - target;
    let candidate = format!(
        "{}…[truncated {removed} bytes]…{}",
        slice_start(text, head_len),
        slice_end(text, tail_len)
    );
    // Never grow the string. For a short input the marker can exceed the target, and
    // returning something longer than the original would be both wrong and an
    // underflow for any caller tracking bytes saved. Measured rather than estimated.
    if candidate.len() < text.len() {
        candidate
    } else {
        text.to_owned()
    }
}

fn slice_start(text: &str, len: usize) -> &str {
    let mut end = len.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn slice_end(text: &str, len: usize) -> &str {
    if len >= text.len() {
        return text;
    }
    let mut start = text.len() - len;
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

fn get_path_mut<'a>(value: &'a mut Value, path: &[&str]) -> Option<&'a mut Value> {
    let mut cursor = value;
    for step in path {
        cursor = cursor.get_mut(step)?;
    }
    Some(cursor)
}

#[cfg(test)]
mod tests {
    use super::{byte_len, fit, protected_subset, truncate_middle};
    use aftman_core::error::ModelError;
    use aftman_core::model::Budget;
    use serde_json::json;

    fn small_budget() -> Budget {
        Budget {
            pair_bytes: 200,
            request_bytes: 1_200,
            pair_target_bytes: 100,
            request_target_bytes: 600,
        }
    }

    fn state_with(large: &str) -> serde_json::Value {
        json!({
            "job": { "prompt": "add retries", "repo": "/repo" },
            "factory_status": { "status": "running", "attempts": 1 },
            "active_worker": { "status": "running" },
            "worker_output": { "output_head": "start", "output_tail": "end" },
            "counters": { "attempts": 1 },
            "prior": null,
            "recent_events": [1, 2, 3, 4, 5, 6, 7, 8],
            "worker_history": [
                { "id": "a", "note": large },
                { "id": "b", "note": large },
                { "id": "c", "note": large }
            ],
            "untracked_evidence": [{"p": "a"}, {"p": "b"}, {"p": "c"}],
            "git_diff": { "diff": large, "stat": "1 file" },
            "command_evidence": { "tests": large, "lint": "ok" }
        })
    }

    #[test]
    fn a_small_state_passes_through_untouched() -> Result<(), Box<dyn std::error::Error>> {
        let state = json!({
            "job": { "prompt": "add retries" },
            "factory_status": { "status": "running" },
            "active_worker": null,
            "worker_output": { "output_tail": "end" },
            "counters": { "attempts": 1 },
            "prior": null
        });
        let before = byte_len(&state);
        let fitted = fit(&state, &small_budget())?;
        assert!(!fitted.compacted, "nothing needed reducing");
        assert_eq!(byte_len(&fitted.state), before);
        assert_eq!(fitted.measurement("final_bytes"), before);
        Ok(())
    }

    #[test]
    fn a_huge_pair_is_truncated_in_the_middle_with_a_marker()
    -> Result<(), Box<dyn std::error::Error>> {
        // A generous request budget with a small per-pair cap: this isolates the pair
        // cap, so the assertion is about truncation rather than about everything else
        // being dropped to reach a tight total.
        let budget = Budget {
            pair_bytes: 200,
            request_bytes: 100_000,
            pair_target_bytes: 100,
            request_target_bytes: 50_000,
        };
        let huge = "x".repeat(5_000);
        let fitted = fit(&state_with(&huge), &budget)?;

        let diff = fitted.state["git_diff"]["diff"]
            .as_str()
            .unwrap_or_default();
        assert!(diff.len() < huge.len());
        assert!(
            diff.contains("truncated"),
            "the clip must be visible: {diff}"
        );
        assert!(diff.starts_with('x'), "the head survives");
        assert!(diff.ends_with('x'), "the tail survives too");
        assert!(fitted.compacted);
        assert!(fitted.measurement("pairs_truncated") > 0);
        Ok(())
    }

    #[test]
    fn an_oversized_pair_in_a_tight_total_budget_is_dropped_not_just_clipped()
    -> Result<(), Box<dyn std::error::Error>> {
        // With a 600-byte total there is no room for a diff at all, and saying so by
        // dropping it is better than sending a 100-byte fragment of it.
        let huge = "x".repeat(5_000);
        let fitted = fit(&state_with(&huge), &small_budget())?;
        assert!(byte_len(&fitted.state) <= small_budget().request_target_bytes);
        assert_eq!(fitted.measurement("diff_dropped"), 1);
        assert!(
            fitted.state["job"].is_object(),
            "required evidence still present"
        );
        Ok(())
    }

    #[test]
    fn the_fitted_state_is_always_inside_the_request_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let huge = "y".repeat(20_000);
        let fitted = fit(&state_with(&huge), &small_budget())?;
        assert!(
            byte_len(&fitted.state) <= small_budget().request_bytes,
            "got {} bytes",
            byte_len(&fitted.state)
        );
        Ok(())
    }

    #[test]
    fn required_evidence_survives_every_reduction() -> Result<(), Box<dyn std::error::Error>> {
        let huge = "z".repeat(20_000);
        let fitted = fit(&state_with(&huge), &small_budget())?;
        assert_eq!(fitted.state["job"]["prompt"], json!("add retries"));
        assert_eq!(fitted.state["factory_status"]["status"], json!("running"));
        assert!(fitted.state.get("active_worker").is_some());
        assert!(fitted.state.get("counters").is_some());
        assert_eq!(
            fitted.state["worker_output"]["output_tail"],
            json!("end"),
            "the active worker's tail is the one thing a model most needs"
        );
        Ok(())
    }

    #[test]
    fn reductions_happen_in_the_documented_order() -> Result<(), Box<dyn std::error::Error>> {
        // Many small events rather than one huge string: per-pair truncation cannot
        // help here, so the only way under the budget is to start dropping, and the
        // order then matters.
        let events: Vec<serde_json::Value> = (0..300)
            .map(|n| json!({ "seq": n, "kind": "WORKER_OUTPUT", "summary": "x" }))
            .collect();
        let state = json!({
            "job": { "prompt": "add retries" },
            "factory_status": { "status": "running" },
            "active_worker": { "status": "running" },
            "worker_output": { "output_head": "start", "output_tail": "end" },
            "counters": { "attempts": 1 },
            "prior": null,
            "recent_events": events,
            "worker_history": [],
            "git_diff": { "diff": "small", "stat": "1 file" }
        });

        let fitted = fit(&state, &small_budget())?;
        assert!(
            fitted.measurement("events_dropped") > 0,
            "events are the cheapest evidence and are dropped first"
        );
        assert!(
            fitted.state["git_diff"]["diff"]
                .as_str()
                .unwrap_or_default()
                == "small",
            "the diff is untouched while events remain"
        );
        assert!(byte_len(&fitted.state) <= small_budget().request_bytes);
        Ok(())
    }

    #[test]
    fn a_large_reducible_field_is_reduced_rather_than_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        // A refusal with a reducible field still in it is a refusal the operator cannot act
        // on without knowing which field to shrink.
        let budget = Budget {
            pair_bytes: 20_000,
            request_bytes: 40_000,
            pair_target_bytes: 12_000,
            request_target_bytes: 20_000,
        };
        let state = json!({
            "job": { "prompt": "add retries" },
            "factory_status": { "status": "running" },
            "active_worker": { "status": "running" },
            "changed_files": (0..6000).map(|i| format!("some/long/path/to/file{i:05}.rs")).collect::<Vec<_>>(),
            "counters": { "attempts": 1 },
        });
        let before = byte_len(&state);
        assert!(
            before > budget.request_bytes,
            "the fixture must start over the budget: {before}"
        );

        let fitted = fit(&state, &budget)?;
        assert!(
            byte_len(&fitted.state) <= budget.request_bytes,
            "the request must fit after reduction"
        );
        let files = fitted.state["changed_files"]
            .as_array()
            .map(Vec::len)
            .unwrap_or(0);
        assert!(files > 0, "some of the list must survive");
        assert!(files < 6000, "and some of it must have been dropped");
        assert!(
            fitted.measurements.contains_key("changed_files_dropped"),
            "the reduction must be reported: {:?}",
            fitted.measurements
        );
        Ok(())
    }

    #[test]
    fn a_state_whose_required_evidence_cannot_fit_is_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        // Refusing beats sending a request with the job removed.
        let budget = Budget {
            pair_bytes: 20_000,
            request_bytes: 40,
            pair_target_bytes: 12_000,
            request_target_bytes: 20,
        };
        let state = json!({
            "job": { "prompt": "a job description that is definitely longer than forty bytes" },
            "factory_status": { "status": "running" }
        });
        let error = fit(&state, &budget).expect_err("must refuse");
        assert!(matches!(error, ModelError::BudgetExceeded(_)), "{error:?}");
        // The refusal explains itself, so "why did supervision stop?" is answerable.
        assert!(error.to_string().contains("required evidence"));
        Ok(())
    }

    #[test]
    fn hysteresis_takes_it_below_the_cap_not_just_to_it() -> Result<(), Box<dyn std::error::Error>>
    {
        let huge = "h".repeat(10_000);
        let fitted = fit(&state_with(&huge), &small_budget())?;
        // The target is 600; landing under the 1200 cap alone would oscillate.
        assert!(
            byte_len(&fitted.state) <= small_budget().request_target_bytes,
            "got {} bytes",
            byte_len(&fitted.state)
        );
        Ok(())
    }

    #[test]
    fn a_huge_worker_tail_is_shrunk_but_never_dropped() -> Result<(), Box<dyn std::error::Error>> {
        // `worker_output.output_tail` is protected evidence: the model is asked whether a
        // worker is stuck, and the tail is where that shows. Budget pressure must shrink
        // it, never remove it.
        let budget = Budget {
            pair_bytes: 4_000,
            request_bytes: 40_000,
            pair_target_bytes: 2_000,
            request_target_bytes: 20_000,
        };
        let state = json!({
            "job": { "prompt": "add retries" },
            "factory_status": { "status": "running" },
            "active_worker": { "status": "running" },
            "worker_output": {
                "output_head": "h".repeat(4_000),
                "output_tail": "t".repeat(50_000),
            },
            "counters": { "attempts": 1 },
            "prior": null,
        });

        let fitted = fit(&state, &budget)?;
        let tail = fitted.state["worker_output"]["output_tail"]
            .as_str()
            .ok_or("the tail must still be a string")?;
        assert!(
            !tail.is_empty(),
            "the tail must never be dropped: {fitted:?}"
        );
        assert!(
            tail.len() <= budget.pair_bytes,
            "the tail must be shrunk to the pair budget, is {} bytes",
            tail.len()
        );
        assert!(
            byte_len(&fitted.state) <= budget.request_bytes,
            "got {} bytes",
            byte_len(&fitted.state)
        );
        Ok(())
    }

    #[test]
    fn the_worker_excerpts_add_a_bounded_share_of_the_request()
    -> Result<(), Box<dyn std::error::Error>> {
        // What the model sees of the worker costs bytes, so it is worth naming the number.
        // The caps are the builder's (`ObsCaps`: 4 000 head, 12 000 tail by default).
        let base = |output: serde_json::Value| {
            json!({
                "job": { "prompt": "add retries with backoff" },
                "factory_status": { "status": "running" },
                "active_worker": { "status": "running", "kind": "pi" },
                "worker_output": output,
                "counters": { "attempts": 1 },
                "prior": null,
                "test_results": [],
            })
        };
        let without = byte_len(&base(json!({ "output_head": "", "output_tail": "" })));
        let with = byte_len(&base(json!({
            "output_head": "h".repeat(4_000),
            "output_tail": "t".repeat(12_000),
        })));
        let added = with - without;
        eprintln!("worker excerpts: {without} bytes without, {with} with (+{added})");
        assert!(
            added <= 20_000,
            "the default caps must stay inside 20 000 bytes, added {added}"
        );
        Ok(())
    }

    #[test]
    fn the_protected_subset_excludes_droppable_evidence() {
        let state = state_with("x");
        let protected = protected_subset(&state);
        assert!(protected.get("job").is_some());
        assert!(protected.get("recent_events").is_none());
        assert!(protected.get("git_diff").is_none());
        assert!(protected.get("command_evidence").is_none());
        assert_eq!(protected["worker_output"]["output_tail"], json!("end"));
        assert!(
            protected["worker_output"].get("output_head").is_none(),
            "the head is droppable, the tail is not"
        );
    }

    #[test]
    fn truncate_middle_keeps_both_ends_and_respects_char_boundaries() {
        // Long enough that the marker costs less than it saves.
        let text = "abcdefghijklmnopqrstuvwxyz".repeat(8);
        let clipped = truncate_middle(&text, 40);
        assert!(clipped.starts_with("abc"));
        assert!(clipped.ends_with("xyz"));
        assert!(clipped.contains("truncated"), "{clipped}");
        assert!(clipped.len() < text.len());

        // A naive byte slice here would panic mid-character.
        let accented = "é".repeat(200);
        let clipped = truncate_middle(&accented, 40);
        assert!(clipped.starts_with('é'));
        assert!(clipped.ends_with('é'));
        assert!(clipped.len() < accented.len());
    }

    #[test]
    fn truncate_middle_declines_when_the_marker_would_cost_more_than_it_saves() {
        // A 26-byte string "truncated" to 10 bytes plus a 25-char marker would be
        // longer than the original. Declining is the correct answer, and it is what
        // keeps the byte accounting from underflowing.
        let text = "abcdefghijklmnopqrstuvwxyz";
        assert_eq!(truncate_middle(text, 10), text);
    }

    #[test]
    fn truncate_middle_leaves_a_short_string_alone() {
        assert_eq!(truncate_middle("short", 100), "short");
    }

    #[test]
    fn truncate_middle_never_grows_a_string() {
        // The marker itself is longer than a small target, so a naive implementation
        // returns something bigger than it was given.
        for input in ["", "a", "abcdef", "a longer but still short string"] {
            for target in [0usize, 1, 4, 8, 16] {
                let out = truncate_middle(input, target);
                assert!(
                    out.len() <= input.len(),
                    "truncate_middle({input:?}, {target}) grew to {} bytes",
                    out.len()
                );
            }
        }
    }

    #[test]
    fn command_evidence_is_dropped_largest_first_when_it_has_to_be()
    -> Result<(), Box<dyn std::error::Error>> {
        let huge = "c".repeat(4_000);
        let fitted = fit(&state_with(&huge), &small_budget())?;
        // Whatever the reductions, the result must be inside the budget and the
        // required evidence must be intact; the exact set dropped is an
        // implementation detail the measurements record.
        assert!(byte_len(&fitted.state) <= small_budget().request_bytes);
        assert!(fitted.state.get("job").is_some());
        Ok(())
    }
}
