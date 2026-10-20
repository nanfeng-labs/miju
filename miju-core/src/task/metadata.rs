//! String metadata that can be persisted alongside a task.
//!
//! Metadata gives integrations a common representation for information such as
//! routing labels and request identifiers, independent of the task's payload.
//! This module separates the stored entries from their application meaning:
//!
//! - [`MetadataStore`] owns the string keys and values, with distinct operations
//!   for inserting new entries and replacing existing ones.
//! - [`Metadata`] defines how an application type reads and writes those entries,
//!   including required keys and validation.
//!
//! A store does not persist itself. Integrations decide how to save and restore
//! its entries. Use [`Extensions`](super::extensions::Extensions) for typed
//! process-local resources that do not need a string representation.
//!
//! # Updating Task Metadata
//!
//! [`TaskBuilder::metadata`](super::builder::TaskBuilder::metadata) adds typed
//! metadata during construction. [`Task::inject_metadata`](super::Task::inject_metadata)
//! updates an existing task and publishes the new entries only after injection
//! succeeds. Direct calls to [`Metadata::inject`] operate on the provided store
//! and may leave partial changes if an implementation fails after writing.
//!
//! See [`Metadata`] for a complete implementation and task update example.

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
};

/// An error returned when a metadata insertion would overwrite an existing key.
///
/// Use [`MetadataStore::replace`] when replacement is intentional.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetadataError {
    /// The key already exists; its original value was retained.
    DuplicateKey(String),
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateKey(key) => write!(f, "metadata key already exists: {key:?}"),
        }
    }
}

impl std::error::Error for MetadataError {}

/// String key/value pairs with explicit insertion and replacement operations.
///
/// Keys and values are stored as provided, including empty strings. Key matching
/// is case-sensitive. Cloning copies all entries, so updates to one store do not
/// modify another. Iteration order is unspecified.
///
/// # Example
///
/// ```
/// use miju_core::task::metadata::{MetadataError, MetadataStore};
///
/// let mut metadata = MetadataStore::new();
/// metadata.insert("region", "west")?;
/// metadata.insert("report", "daily")?;
///
/// assert_eq!(metadata.get("region"), Some("west"));
/// assert_eq!(metadata.get("missing"), None);
/// assert_eq!(metadata.len(), 2);
/// # Ok::<(), MetadataError>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetadataStore {
    values: HashMap<String, String>,
}

impl MetadataStore {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a new key, preserving the existing value on a duplicate.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::DuplicateKey`] if the key already exists.
    /// The existing entry is retained, even when the new value is identical.
    ///
    /// # Example
    ///
    /// ```
    /// # use miju_core::task::metadata::{MetadataError, MetadataStore};
    /// let mut metadata = MetadataStore::new();
    /// metadata.insert("region", "west")?;
    /// assert_eq!(
    ///     metadata.insert("region", "east"),
    ///     Err(MetadataError::DuplicateKey("region".to_owned())),
    /// );
    /// assert_eq!(metadata.get("region"), Some("west"));
    /// # Ok::<(), MetadataError>(())
    /// ```
    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), MetadataError> {
        match self.values.entry(key.into()) {
            Entry::Occupied(entry) => Err(MetadataError::DuplicateKey(entry.key().clone())),
            Entry::Vacant(entry) => {
                entry.insert(value.into());
                Ok(())
            }
        }
    }

    /// Borrows the value for a key.
    ///
    /// Returns `None` when the key is absent. An empty stored value is returned
    /// as `Some("")`, rather than being treated as missing.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// Removes a key and returns its previous value.
    ///
    /// Returns `None` when the key is absent; other entries are unchanged.
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.values.remove(key)
    }

    /// Inserts or overwrites a key, returning any previous value.
    ///
    /// # Example
    ///
    /// ```
    /// # use miju_core::task::metadata::MetadataStore;
    /// let mut metadata = MetadataStore::new();
    /// assert_eq!(metadata.replace("region", "west"), None);
    /// assert_eq!(metadata.replace("region", "east"), Some("west".to_owned()));
    /// assert_eq!(metadata.get("region"), Some("east"));
    /// ```
    pub fn replace(&mut self, key: impl Into<String>, value: impl Into<String>) -> Option<String> {
        self.values.insert(key.into(), value.into())
    }

    /// Inserts entries, overwriting existing keys. Later entries take precedence.
    ///
    /// Unlike [`insert`](Self::insert), this operation accepts duplicate keys.
    /// That includes duplicates within the supplied iterator.
    ///
    /// # Example
    ///
    /// ```
    /// # use miju_core::task::metadata::MetadataStore;
    /// let mut metadata = MetadataStore::new();
    /// metadata.extend([("region", "west"), ("report", "daily"), ("region", "east")]);
    /// assert_eq!(metadata.get("region"), Some("east"));
    /// assert_eq!(metadata.len(), 2);
    /// ```
    pub fn extend<K: Into<String>, V: Into<String>>(
        &mut self,
        values: impl IntoIterator<Item = (K, V)>,
    ) {
        self.values.extend(
            values
                .into_iter()
                .map(|(key, value)| (key.into(), value.into())),
        );
    }

    /// Borrows all entries in unspecified order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }

    /// Returns the number of keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns whether no keys are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Maps an application type to entries in a [`MetadataStore`].
///
/// Implementations choose their keys, representation, and validation rules.
/// Coordinate key names when several metadata types share a store. Extraction
/// borrows the store; injection may modify it and is not inherently transactional.
/// [`Task::inject_metadata`](super::Task::inject_metadata) stages those writes
/// and only updates the task after success.
///
/// # Example
///
/// Define a region label that rejects empty values and duplicate keys, then
/// attach it to a task and read it back:
///
/// ```
/// use miju_core::task::{Task, metadata::{Metadata, MetadataStore}};
///
/// #[derive(Debug, PartialEq, Eq)]
/// struct Region(String);
///
/// impl Metadata for Region {
///     type Error = &'static str;
///
///     fn extract(store: &MetadataStore) -> Result<Self, Self::Error> {
///         let value = store.get("region").ok_or("missing region")?;
///         if value.is_empty() {
///             return Err("empty region");
///         }
///         Ok(Self(value.to_owned()))
///     }
///
///     fn inject(&self, store: &mut MetadataStore) -> Result<(), Self::Error> {
///         if self.0.is_empty() {
///             return Err("empty region");
///         }
///         store.insert("region", self.0.clone()).map_err(|_| "duplicate region")
///     }
/// }
///
/// let mut task = Task::new("generate report");
/// task.inject_metadata(&Region("west".to_owned()))?;
/// assert_eq!(Region::extract(task.metadata())?, Region("west".to_owned()));
///
/// assert!(task.inject_metadata(&Region("east".to_owned())).is_err());
/// assert_eq!(task.metadata().get("region"), Some("west"));
/// # Ok::<(), &'static str>(())
/// ```
pub trait Metadata: Sized {
    /// A validation, extraction, or insertion failure.
    ///
    /// The implementation selects a type that can describe both read and write
    /// failures, including missing keys or a rejected stored representation.
    type Error;

    /// Reads and validates a typed value from the store.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when required entries are missing or their values
    /// do not satisfy the implementation's format and validation rules.
    fn extract(store: &MetadataStore) -> Result<Self, Self::Error>;

    /// Writes the value's entries to the store.
    ///
    /// # Errors
    ///
    /// Returns [`Self::Error`] when the value or an insertion is rejected.
    /// Earlier writes may remain in `store` on failure; callers that require
    /// all-or-nothing updates must stage them, as [`Task::inject_metadata`](super::Task::inject_metadata)
    /// does.
    fn inject(&self, store: &mut MetadataStore) -> Result<(), Self::Error>;
}
