//! RFC 3339 formatting for timestamps.
//!
//! `time`'s default `serde` representation is a struct of components, which is
//! unreadable in a JSONL event log. Everything that lands on disk or on the wire
//! uses an RFC 3339 string instead.

use serde::{Deserialize, Deserializer, Serializer};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub fn serialize<S>(value: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let text = value.format(&Rfc3339).map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&text)
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let text = String::deserialize(deserializer)?;
    OffsetDateTime::parse(&text, &Rfc3339).map_err(serde::de::Error::custom)
}

pub fn serialize_opt<S>(value: &Option<OffsetDateTime>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(at) => serialize(at, serializer),
        None => serializer.serialize_none(),
    }
}

pub fn deserialize_opt<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
where
    D: Deserializer<'de>,
{
    let text = Option::<String>::deserialize(deserializer)?;
    match text {
        Some(text) => OffsetDateTime::parse(&text, &Rfc3339)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

/// `#[serde(with = "crate::timefmt::opt")]` for optional timestamps.
pub mod opt {
    pub use super::{deserialize_opt as deserialize, serialize_opt as serialize};
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;
    use time::macros::datetime;

    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug)]
    struct Wrap {
        #[serde(with = "crate::timefmt")]
        at: OffsetDateTime,
        #[serde(with = "crate::timefmt::opt", default)]
        maybe: Option<OffsetDateTime>,
    }

    #[test]
    fn timestamps_round_trip_as_rfc3339_strings() -> Result<(), Box<dyn std::error::Error>> {
        let value = Wrap {
            at: datetime!(2026-01-02 03:04:05.123 UTC),
            maybe: Some(datetime!(2026-01-02 03:04:05 UTC)),
        };
        let json = serde_json::to_string(&value)?;
        assert!(json.contains("\"2026-01-02T03:04:05.123Z\""), "{json}");
        assert_eq!(serde_json::from_str::<Wrap>(&json)?, value);
        Ok(())
    }

    #[test]
    fn optional_timestamps_accept_null() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{"at":"2026-01-02T03:04:05Z","maybe":null}"#;
        let parsed: Wrap = serde_json::from_str(json)?;
        assert_eq!(parsed.maybe, None);
        let round = serde_json::to_string(&parsed)?;
        assert!(round.contains("null"), "{round}");
        Ok(())
    }

    #[test]
    fn a_missing_optional_field_defaults_to_none() -> Result<(), Box<dyn std::error::Error>> {
        let parsed: Wrap = serde_json::from_str(r#"{"at":"2026-01-02T03:04:05Z"}"#)?;
        assert_eq!(parsed.maybe, None);
        Ok(())
    }
}
