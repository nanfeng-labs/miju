#[derive(Debug, thiserror::Error)]
pub(crate) enum DeliveryError {
    #[error("delivery panicked: {0}")]
    Panic(String),
    #[error("cached address is invalid")]
    InvalidEmail,
    #[error("simulated API quota exhausted")]
    ApiRateLimit,
}
