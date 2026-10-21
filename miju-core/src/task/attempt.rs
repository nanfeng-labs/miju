//! A thread-safe tracker for counting the number of attempts made by a task.
//!
//! The `Attempt` struct wraps an atomic counter, allowing concurrent increment and retrieval of the attempt count. It is designed to be used within the Miju job/task system, enabling tasks to keep track of how many times they have been retried or executed.
//!
//! Features:
//! - Thread-safe increment and retrieval of attempt count.
//! - Optional (via the `serde` feature) serialization and deserialization support for persisting or transmitting attempt state.
use std::sync::{Arc, atomic::AtomicUsize};

/// A wrapper to keep count of the attempts tried by a task
#[derive(Debug, Clone)]
pub struct Attempt(Arc<AtomicUsize>);

impl Default for Attempt {
    fn default() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }
}

impl Attempt {
    /// Build a new tracker
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a tracker from an existing value
    #[must_use]
    pub fn new_with_value(value: usize) -> Self {
        Self(Arc::new(AtomicUsize::from(value)))
    }

    /// Get the current value
    #[must_use]
    pub fn current(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Increase the current value and return the previous value
    #[must_use]
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Attempt updates are reserved for internal execution drivers"
        )
    )]
    pub(crate) fn increment(&self) -> usize {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(feature = "serde")]
mod serde_impl {
    use std::sync::atomic::Ordering;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::*;

    // Custom serialization function
    fn serialize<S>(attempt: &Attempt, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let value = attempt.0.load(Ordering::SeqCst);
        serializer.serialize_u64(value as u64)
    }

    // Custom deserialization function
    fn deserialize<'de, D>(deserializer: D) -> Result<Attempt, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Ok(Attempt(Arc::new(AtomicUsize::new(value as usize))))
    }

    impl Serialize for Attempt {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serialize(self, serializer)
        }
    }

    impl<'de> Deserialize<'de> for Attempt {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserialize(deserializer)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Attempt;

    #[test]
    fn concurrent_attempts_return_each_previous_value_once() {
        let attempts = Attempt::new_with_value(7);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let counter = attempts.clone();
                std::thread::spawn(move || {
                    (0..500).map(|_| counter.increment()).collect::<Vec<_>>()
                })
            })
            .collect();
        let mut observed: Vec<_> = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect();
        observed.sort_unstable();
        assert_eq!(observed, (7..4_007).collect::<Vec<_>>());
        assert_eq!(attempts.current(), 4_007);
        assert_eq!(Attempt::new().current(), 0);
    }

    #[test]
    fn attempt_overflow_matches_atomic_wrapping_semantics() {
        let attempts = Attempt::new_with_value(usize::MAX);
        assert_eq!(attempts.increment(), usize::MAX);
        assert_eq!(attempts.current(), 0);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn deserializing_an_attempt_creates_an_independent_counter() {
        let original = Attempt::new_with_value(7);
        let snapshot: Attempt =
            serde_json::from_value(serde_json::to_value(&original).unwrap()).unwrap();
        assert_eq!(original.increment(), 7);
        assert_eq!(original.current(), 8);
        assert_eq!(snapshot.current(), 7);
    }
}
