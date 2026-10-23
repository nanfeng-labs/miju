//! Shared email payloads and simulated delivery for the runnable examples.
//!
//! Address validation selects success, an ordinary error, or an [`AbortError`].
//! No SMTP connection is opened and no message is sent. The retry examples use
//! these outcomes to show how a policy treats different errors.

use miju::prelude::{AbortError, BoxDynError};
use serde::{Deserialize, Serialize};

/// A serializable email job used by memory, file storage, and HTTP examples.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Email {
    /// Recipient address to validate.
    pub to: String,
    /// Subject displayed by the example application.
    pub subject: String,
    /// Plain-text message body.
    pub text: String,
}

fn validate_address(address: &str) -> Result<(), BoxDynError> {
    match address.parse::<email_address::EmailAddress>() {
        Ok(_) => Ok(()),
        Err(email_address::Error::InvalidCharacter) => {
            Err(AbortError::new("recipient contains an invalid character").into())
        }
        Err(error) => Err(error.into()),
    }
}

/// Validate a recipient and simulate successful delivery.
///
/// Invalid characters produce [`AbortError`]; other parsing errors remain ordinary
/// errors so an example can demonstrate retries. Retrying an unchanged invalid
/// address cannot repair it; this classification is for illustrating retry policies.
pub async fn send_email(email: Email) -> Result<(), BoxDynError> {
    validate_address(&email.to)?;
    tracing::info!(
        recipient = email.to,
        "Email accepted for simulated delivery"
    );
    Ok(())
}

/// A payload whose recipient passes validation.
#[must_use]
pub fn example_good_email() -> Email {
    Email {
        to: "reader@example.com".to_owned(),
        subject: "Example notification".to_owned(),
        text: "A simulated background message".to_owned(),
    }
}

/// A payload whose invalid character makes delivery return [`AbortError`].
#[must_use]
pub fn example_killed_email() -> Email {
    Email {
        to: "reader@©.com".to_owned(),
        ..example_good_email()
    }
}

/// A payload whose missing address separator produces an ordinary error.
#[must_use]
pub fn example_retry_able_email() -> Email {
    Email {
        to: "reader.example.com".to_owned(),
        ..example_good_email()
    }
}

/// A self-contained form posting the three [`Email`] fields to the current server.
pub const FORM_HTML: &str = r#"<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Queue an email</title>
<style>body{font:1rem system-ui;max-width:36rem;margin:3rem auto;padding:0 1rem}
label{display:block;margin:1rem 0}input,textarea,button{font:inherit;padding:.5rem;box-sizing:border-box}
input,textarea{display:block;width:100%}button{cursor:pointer}</style></head>
<body><h1>Queue a simulated email</h1><p>Jobs validate addresses without sending mail.</p>
<form method="post" action="/">
<label>Recipient<input name="to" type="email" placeholder="reader@example.com" required></label>
<label>Subject<input name="subject" required></label>
<label>Message<textarea name="text" rows="6" required></textarea></label>
<button type="submit">Queue job</button></form></body></html>"#;

/// Application errors available to examples that manage email backends.
#[derive(Debug)]
#[non_exhaustive]
pub enum EmailError {
    /// A requested backend is unavailable.
    NoStorage,
    /// A fixed message describing an example-specific failure.
    SomeError(&'static str),
}

impl std::fmt::Display for EmailError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoStorage => formatter.write_str("email storage is unavailable"),
            Self::SomeError(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for EmailError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_outcomes_remain_distinguishable() {
        assert!(validate_address(&example_good_email().to).is_ok());
        let retryable = validate_address(&example_retry_able_email().to).unwrap_err();
        assert!(retryable.downcast_ref::<AbortError>().is_none());
        for recipient in [example_killed_email().to, "A@b@c@example.com".to_owned()] {
            let aborted = validate_address(&recipient).unwrap_err();
            assert!(aborted.downcast_ref::<AbortError>().is_some());
        }
    }
}
