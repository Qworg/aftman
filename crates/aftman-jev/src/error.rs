//! Error classification and message extraction.
//!
//! The provider's error bodies have several shapes, and an operator needs one useful
//! sentence from them. The extraction order here is copied from the SDK's own
//! behaviour so that a failure reads the same whichever client produced it. See
//! `docs/03-jev.md` §4.

use std::time::Duration;

use aftman_core::error::ModelError;
use serde_json::Value;

/// How much of an error body is retained. Bodies can be large and are never the point.
pub const MAX_ERROR_BODY: usize = 200;

/// Extract a human message from an error body.
///
/// Tried in order: `error` (string, or an object with a `message`), `message`,
/// `detail` (string, or an object with a `message`, or a validation list rendered as
/// `field.path: msg; …`).
#[must_use]
pub fn extract_message(body: &[u8]) -> Option<String> {
    if body.is_empty() {
        return None;
    }
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        // A non-JSON body is still worth reporting, truncated.
        return Some(truncate(&String::from_utf8_lossy(body), MAX_ERROR_BODY));
    };
    if let Some(text) = value.as_str() {
        return non_empty(text);
    }
    let object = value.as_object()?;

    if let Some(error) = object.get("error") {
        if let Some(text) = error.as_str() {
            return non_empty(text);
        }
        if let Some(text) = error.get("message").and_then(Value::as_str) {
            return non_empty(text);
        }
    }
    if let Some(text) = object.get("message").and_then(Value::as_str) {
        return non_empty(text);
    }
    if let Some(detail) = object.get("detail") {
        if let Some(text) = detail.as_str() {
            return non_empty(text);
        }
        if let Some(text) = detail.get("message").and_then(Value::as_str) {
            return non_empty(text);
        }
        if let Some(entries) = detail.as_array() {
            let rendered: Vec<String> = entries
                .iter()
                .filter_map(|entry| {
                    let message = entry.get("msg")?.as_str()?;
                    let path = entry
                        .get("loc")
                        .and_then(Value::as_array)
                        .map(|parts| {
                            parts
                                .iter()
                                .filter_map(Value::as_str)
                                .filter(|part| *part != "body")
                                .collect::<Vec<_>>()
                                .join(".")
                        })
                        .unwrap_or_default();
                    Some(if path.is_empty() {
                        message.to_owned()
                    } else {
                        format!("{path}: {message}")
                    })
                })
                .collect();
            if !rendered.is_empty() {
                return Some(truncate(&rendered.join("; "), MAX_ERROR_BODY));
            }
        }
    }
    None
}

fn non_empty(text: &str) -> Option<String> {
    if text.trim().is_empty() {
        None
    } else {
        Some(truncate(text, MAX_ERROR_BODY))
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// Turn an unsuccessful HTTP response into a [`ModelError`].
///
/// The mapping is the error table in `docs/03-jev.md` §4. Retryability is a property
/// of the variant, not decided here.
#[must_use]
pub fn classify_status(status: u16, body: &[u8]) -> ModelError {
    let message = extract_message(body).unwrap_or_else(|| format!("HTTP {status}"));
    match status {
        400 | 422 => ModelError::BadRequest(message),
        401 | 403 => ModelError::Auth,
        404 => ModelError::Endpoint,
        429 => ModelError::RateLimited { retry_after: None },
        // 408 is a server-side timeout; treat it as retryable like a 5xx.
        408 | 500..=599 => ModelError::Server { status, message },
        _ => ModelError::Server { status, message },
    }
}

/// Attach a parsed `Retry-After` to a rate-limit error.
#[must_use]
pub fn with_retry_after(error: ModelError, retry_after: Option<Duration>) -> ModelError {
    match error {
        ModelError::RateLimited { .. } => ModelError::RateLimited { retry_after },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_ERROR_BODY, classify_status, extract_message, with_retry_after};
    use aftman_core::error::ModelError;
    use std::time::Duration;

    #[test]
    fn an_error_string_is_used_verbatim() {
        assert_eq!(
            extract_message(br#"{"error":"model overloaded"}"#),
            Some("model overloaded".to_owned())
        );
    }

    #[test]
    fn a_nested_error_message_is_found() {
        assert_eq!(
            extract_message(br#"{"error":{"message":"bad question","type":"invalid"}}"#),
            Some("bad question".to_owned())
        );
    }

    #[test]
    fn a_plain_message_is_found() {
        assert_eq!(
            extract_message(br#"{"message":"nope"}"#),
            Some("nope".to_owned())
        );
    }

    #[test]
    fn a_validation_list_is_rendered_with_field_paths() -> Result<(), Box<dyn std::error::Error>> {
        let body = br#"{"detail":[{"loc":["body","questions","urgency","score","criteria"],"msg":"Field required"},{"loc":["body","state"],"msg":"State is required."}]}"#;
        let message = extract_message(body).ok_or("a message")?;
        assert!(
            message.contains("questions.urgency.score.criteria: Field required"),
            "{message}"
        );
        assert!(message.contains("state: State is required."), "{message}");
        assert!(
            !message.contains("body."),
            "the `body` prefix is noise: {message}"
        );
        Ok(())
    }

    #[test]
    fn a_non_json_body_is_still_reported() -> Result<(), Box<dyn std::error::Error>> {
        let message = extract_message(b"<html>502 Bad Gateway</html>").ok_or("a message")?;
        assert!(message.contains("502"));
        Ok(())
    }

    #[test]
    fn an_empty_body_yields_nothing() {
        assert_eq!(extract_message(b""), None);
        assert_eq!(extract_message(br#"{"unrelated":1}"#), None);
        assert_eq!(extract_message(br#"{"message":"   "}"#), None);
    }

    #[test]
    fn a_huge_body_is_truncated_on_a_character_boundary() -> Result<(), Box<dyn std::error::Error>>
    {
        let long = "é".repeat(1000);
        let body = format!(r#"{{"message":"{long}"}}"#);
        let message = extract_message(body.as_bytes()).ok_or("a message")?;
        assert!(message.len() <= MAX_ERROR_BODY + 4, "len {}", message.len());
        assert!(message.ends_with('…'));
        Ok(())
    }

    #[test]
    fn statuses_map_to_the_documented_errors() {
        assert!(matches!(
            classify_status(400, br#"{"message":"nope"}"#),
            ModelError::BadRequest(_)
        ));
        assert!(matches!(
            classify_status(422, br#"{"message":"nope"}"#),
            ModelError::BadRequest(_)
        ));
        assert!(matches!(classify_status(401, b""), ModelError::Auth));
        assert!(matches!(classify_status(403, b""), ModelError::Auth));
        assert!(matches!(classify_status(404, b""), ModelError::Endpoint));
        assert!(matches!(
            classify_status(429, b""),
            ModelError::RateLimited { .. }
        ));
        assert!(matches!(
            classify_status(503, br#"{"message":"try later"}"#),
            ModelError::Server { status: 503, .. }
        ));
    }

    #[test]
    fn retryability_follows_the_classification() {
        assert!(!classify_status(400, b"").is_retryable());
        assert!(!classify_status(401, b"").is_retryable());
        assert!(classify_status(429, b"").is_retryable());
        assert!(classify_status(500, b"").is_retryable());
        assert!(classify_status(408, b"").is_retryable());
        assert!(ModelError::Timeout(Duration::from_secs(1)).is_retryable());
        assert!(!ModelError::MissingAnswer("x".into()).is_retryable());
    }

    #[test]
    fn a_parsed_retry_after_is_attached_to_a_rate_limit() {
        let error = with_retry_after(classify_status(429, b""), Some(Duration::from_millis(100)));
        assert_eq!(
            error,
            ModelError::RateLimited {
                retry_after: Some(Duration::from_millis(100))
            }
        );
    }

    #[test]
    fn attaching_a_retry_after_to_another_error_is_a_no_op() {
        let error = with_retry_after(classify_status(500, b""), Some(Duration::from_secs(1)));
        assert!(matches!(error, ModelError::Server { status: 500, .. }));
    }

    #[test]
    fn error_codes_are_stable() {
        assert_eq!(classify_status(401, b"").code(), "auth");
        assert_eq!(classify_status(429, b"").code(), "rate_limited");
        assert_eq!(classify_status(503, b"").code(), "server");
        assert_eq!(classify_status(422, b"").code(), "bad_request");
    }
}
