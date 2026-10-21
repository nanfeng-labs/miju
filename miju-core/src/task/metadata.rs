//! Task metadata extension trait and implementations
//!
//! ## Overview
//! - `Metadata`: A trait for extracting and injecting metadata.
use std::{
    collections::HashMap,
    ops::{Index, IndexMut},
};

/// A lightweight key-value metadata store backed by a `HashMap`.
///
/// `MetadataStore` is designed for storing arbitrary string metadata such as
/// task attributes, labels, annotations, headers, or contextual information.
///
/// Keys are unique within the store. Attempting to insert a duplicate key
/// returns a [`MetadataError::DuplicateKey`] error.
///
/// # Examples
///
/// ```rust
/// # use std::collections::HashMap;
/// # use miju_core::task::metadata::Metadata;
/// # use miju_core::task::metadata::MetadataStore;
/// # use miju_core::task::metadata::MetadataError;
/// #
/// let mut metadata = MetadataStore::new();
///
/// metadata.insert("request_id", "abc-123")?;
/// metadata.insert("environment", "production")?;
///
/// assert_eq!(
///     metadata.get("request_id"),
///     Some("abc-123")
/// );
///
/// assert!(metadata.contains_key("environment"));
///
/// # Ok::<(), MetadataError>(())
/// ```
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataStore(HashMap<String, String>);

/// Errors returned by [`MetadataStore`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum MetadataError {
    /// Returned when attempting to insert a key that already exists.
    #[error("The key already exists in the store")]
    DuplicateKey(String),
}

impl MetadataStore {
    /// Creates an empty [`MetadataStore`].
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use miju_core::task::metadata::Metadata;
    /// # use miju_core::task::metadata::MetadataStore;
    /// let metadata = MetadataStore::new();
    ///
    /// assert_eq!(metadata.iter().count(), 0);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Inserts a key-value pair into the store.
    ///
    /// Returns an error if the key already exists.
    ///
    /// # Errors
    ///
    /// Returns [`MetadataError::DuplicateKey`] if the provided key is already
    /// present in the store.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use miju_core::task::metadata::Metadata;
    /// # use miju_core::task::metadata::MetadataStore;
    /// # use miju_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("region", "us-east-1")?;
    ///
    /// assert_eq!(
    ///     metadata.get("region"),
    ///     Some("us-east-1")
    /// );
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    ///
    /// Duplicate keys are rejected:
    ///
    /// ```rust
    /// # use miju_core::task::metadata::Metadata;
    /// # use miju_core::task::metadata::MetadataStore;
    /// # use miju_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("service", "api")?;
    ///
    /// let err = metadata.insert("service", "worker").unwrap_err();
    ///
    /// assert_eq!(
    ///     err,
    ///     MetadataError::DuplicateKey("service".to_string())
    /// );
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    pub fn insert<K, V>(&mut self, key: K, value: V) -> Result<(), MetadataError>
    where
        K: Into<String>,
        V: Into<String>,
    {
        let key = key.into();

        if self.0.contains_key(&key) {
            return Err(MetadataError::DuplicateKey(key));
        }

        self.0.insert(key, value.into());

        Ok(())
    }

    /// Returns a reference to the value corresponding to the given key.
    ///
    /// Returns `None` if the key does not exist.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use miju_core::task::metadata::Metadata;
    /// # use miju_core::task::metadata::MetadataStore;
    /// # use miju_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("version", "1.0")?;
    ///
    /// assert_eq!(
    ///     metadata.get("version"),
    ///     Some("1.0")
    /// );
    ///
    /// assert_eq!(metadata.get("missing"), None);
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    /// Removes a key from the store, returning the stored value if it existed.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use miju_core::task::metadata::Metadata;
    /// # use miju_core::task::metadata::MetadataStore;
    /// # use miju_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("token", "secret")?;
    ///
    /// assert_eq!(
    ///     metadata.remove("token"),
    ///     Some("secret".to_string())
    /// );
    ///
    /// assert!(!metadata.contains_key("token"));
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.0.remove(key)
    }

    /// Returns `true` if the store contains the specified key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use miju_core::task::metadata::Metadata;
    /// # use miju_core::task::metadata::MetadataStore;
    /// # use miju_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("owner", "alice")?;
    ///
    /// assert!(metadata.contains_key("owner"));
    /// assert!(!metadata.contains_key("missing"));
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    /// Returns an iterator over all key-value pairs in the store.
    ///
    /// The iterator yields `(&String, &String)` pairs.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use miju_core::task::metadata::Metadata;
    /// # use miju_core::task::metadata::MetadataStore;
    /// # use miju_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("a", "1")?;
    /// metadata.insert("b", "2")?;
    ///
    /// let items: Vec<_> = metadata.iter().collect();
    ///
    /// assert_eq!(items.len(), 2);
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.0.iter()
    }

    /// Consumes the store and returns the underlying `HashMap`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use miju_core::task::metadata::Metadata;
    /// # use miju_core::task::metadata::MetadataStore;
    /// # use miju_core::task::metadata::MetadataError;
    /// let mut metadata = MetadataStore::new();
    ///
    /// metadata.insert("key", "value")?;
    ///
    /// let inner = metadata.into_inner();
    ///
    /// assert_eq!(
    ///     inner.get("key"),
    ///     Some(&"value".to_string())
    /// );
    ///
    /// # Ok::<(), MetadataError>(())
    /// ```
    #[must_use]
    pub fn into_inner(self) -> HashMap<String, String> {
        self.0
    }

    /// Get a typed metadata entry.
    #[must_use = "Extracted metadata should be used or handled"]
    pub fn extract_as<M: Metadata>(&self) -> Result<M, M::Error> {
        M::extract(self)
    }

    /// Create a `MetadataStore` from a `HashMap<String, String>`.
    #[must_use]
    pub fn from_map(map: HashMap<String, String>) -> Self {
        Self(map)
    }

    /// Returns the number of entries in the store.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the store contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Removes all entries from the store.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Returns a mutable reference to the value corresponding to the key.
    ///
    /// Returns `None` if the key does not exist.
    #[must_use]
    pub fn get_mut(&mut self, key: &str) -> Option<&mut String> {
        self.0.get_mut(key)
    }

    /// Returns a mutable iterator over all key-value pairs in the store.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut String)> {
        self.0.iter_mut()
    }

    /// Returns an iterator over all keys in the store.
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.0.keys()
    }

    /// Returns an iterator over all values in the store.
    pub fn values(&self) -> impl Iterator<Item = &String> {
        self.0.values()
    }

    /// Returns a mutable iterator over all values in the store.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut String> {
        self.0.values_mut()
    }

    /// Extends the store with key-value pairs.
    ///
    /// Existing keys are overwritten.
    ///
    /// Use [`MetadataStore::insert`] when duplicate keys should be rejected.
    pub fn extend<I, K, V>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.0.extend(
            iter.into_iter()
                .map(|(key, value)| (key.into(), value.into())),
        );
    }

    /// Returns a reference to the underlying map.
    #[must_use]
    pub fn as_map(&self) -> &HashMap<String, String> {
        &self.0
    }

    /// Inserts or updates a value.
    ///
    /// Returns `None` if the key does not exist.
    pub fn replace<K, V>(&mut self, key: K, value: V) -> Option<String>
    where
        K: AsRef<str>,
        V: Into<String>,
    {
        self.0.insert(key.as_ref().to_owned(), value.into())
    }

    /// Inserts a value only if the key does not already exist.
    pub fn insert_if_absent<K, V>(&mut self, key: K, value: V) -> bool
    where
        K: Into<String>,
        V: Into<String>,
    {
        let key = key.into();

        if let std::collections::hash_map::Entry::Vacant(e) = self.0.entry(key) {
            e.insert(value.into());
            true
        } else {
            false
        }
    }
}

impl<K, V> FromIterator<(K, V)> for MetadataStore
where
    K: Into<String>,
    V: Into<String>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        )
    }
}

impl IntoIterator for MetadataStore {
    type Item = (String, String);
    type IntoIter = std::collections::hash_map::IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a MetadataStore {
    type Item = (&'a String, &'a String);
    type IntoIter = std::collections::hash_map::Iter<'a, String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a> IntoIterator for &'a mut MetadataStore {
    type Item = (&'a String, &'a mut String);
    type IntoIter = std::collections::hash_map::IterMut<'a, String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

impl AsRef<HashMap<String, String>> for MetadataStore {
    fn as_ref(&self) -> &HashMap<String, String> {
        &self.0
    }
}

impl AsMut<HashMap<String, String>> for MetadataStore {
    fn as_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.0
    }
}

impl Index<&str> for MetadataStore {
    type Output = String;

    fn index(&self, key: &str) -> &Self::Output {
        &self.0[key]
    }
}

impl IndexMut<&str> for MetadataStore {
    fn index_mut(&mut self, key: &str) -> &mut Self::Output {
        self.0
            .get_mut(key)
            .unwrap_or_else(|| panic!("metadata key `{key}` does not exist"))
    }
}

/// Implemented by types that can be stored as metadata.
/// Provides a stable key and string-based serialization.
pub trait Metadata: Sized {
    /// The error produced when extracting the Metadata
    type Error;

    /// Extract `Metadata` from the store
    fn extract(store: &MetadataStore) -> Result<Self, Self::Error>;

    /// Inject [`Self`] into the store
    fn inject(&self, map: &mut MetadataStore) -> Result<(), Self::Error>;
}
