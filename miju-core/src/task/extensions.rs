//! Typed values attached to tasks within the current process.
//!
//! [`Extensions`] associates a value with its concrete Rust type. Use it to
//! carry application resources or extra context without adding them to the
//! task's payload. Values are kept in memory without a serialization format;
//! use [`MetadataStore`](super::metadata::MetadataStore) for string metadata
//! that a storage integration can persist.
//!
//! # Type-Based Lookup
//!
//! Each type has one entry. Inserting another value of that type replaces the
//! entry, while different types remain independent. Wrapper types distinguish
//! multiple values that otherwise share a representation:
//!
//! ```
//! use miju_core::task::extensions::Extensions;
//!
//! #[derive(Clone)]
//! struct Region(String);
//! #[derive(Clone)]
//! struct RequestLabel(String);
//!
//! let mut data = Extensions::new();
//! data.insert(Region("west".to_owned()));
//! data.insert(RequestLabel("daily-report".to_owned()));
//!
//! assert_eq!(data.get::<Region>().unwrap().0, "west");
//! assert_eq!(data.get::<RequestLabel>().unwrap().0, "daily-report");
//! assert!(data.get::<String>().is_none());
//! ```
//!
//! [`TaskBuilder::data`](super::builder::TaskBuilder::data) attaches values during
//! construction. [`Task::inject_data`](super::Task::inject_data) updates them
//! later, detaching a shared task context before changing its entries.

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    fmt,
};

type Value = dyn Any + Send + Sync;

struct Entry {
    value: Box<Value>,
    // Keep the concrete cloning operation available after erasing the type.
    clone_value: fn(&Value) -> Box<Value>,
}

impl Entry {
    fn new<T: Clone + Send + Sync + 'static>(value: T) -> Self {
        Self {
            value: Box::new(value),
            clone_value: |value| {
                Box::new(
                    value
                        .downcast_ref::<T>()
                        .expect("entry retains its original type")
                        .clone(),
                )
            },
        }
    }

    fn into_value<T: Send + Sync + 'static>(self) -> T {
        *self
            .value
            .downcast::<T>()
            .expect("entry is indexed by its stored type")
    }
}

impl Clone for Entry {
    fn clone(&self) -> Self {
        Self {
            value: (self.clone_value)(self.value.as_ref()),
            clone_value: self.clone_value,
        }
    }
}

/// Stores one value per concrete type, without serializing it.
///
/// Stored types must implement [`Clone`], [`Send`], and [`Sync`] and contain no
/// non-static borrows. Retrieval uses the exact stored type rather than an
/// implicit conversion to another type.
///
/// # Cloning
///
/// Cloning calls each value's [`Clone`] implementation. For example, cloning a
/// `Vec` copies its elements, while cloning an `Arc` continues to share its data.
///
/// ```
/// use std::sync::Arc;
/// use miju_core::task::extensions::Extensions;
///
/// let label = Arc::new("catalog".to_owned());
/// let mut original = Extensions::new();
/// original.insert(vec![1_u32]);
/// original.insert(Arc::clone(&label));
///
/// let mut copy = original.clone();
/// copy.get_mut::<Vec<u32>>().unwrap().push(2);
/// assert_eq!(original.get::<Vec<u32>>().unwrap(), &[1]);
/// assert_eq!(copy.get::<Vec<u32>>().unwrap(), &[1, 2]);
/// assert!(Arc::ptr_eq(copy.get::<Arc<String>>().unwrap(), &label));
/// ```
#[derive(Clone, Default)]
pub struct Extensions {
    entries: HashMap<TypeId, Entry>,
}

impl Extensions {
    /// Creates an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces the value of the same type and returns the previous value.
    ///
    /// Returns `None` when that type has no entry. Replacement affects only the
    /// exact type of `value`.
    ///
    /// # Example
    ///
    /// ```
    /// # use miju_core::task::extensions::Extensions;
    /// let mut data = Extensions::new();
    /// assert_eq!(data.insert(10_u32), None);
    /// assert_eq!(data.insert(20_u32), Some(10));
    /// assert_eq!(data.insert(30_u64), None);
    /// assert_eq!(data.len(), 2);
    /// ```
    pub fn insert<T: Clone + Send + Sync + 'static>(&mut self, value: T) -> Option<T> {
        self.entries
            .insert(TypeId::of::<T>(), Entry::new(value))
            .map(Entry::into_value)
    }

    /// Borrows the value of the requested type, if present.
    ///
    /// # Example
    ///
    /// ```
    /// # use miju_core::task::extensions::Extensions;
    /// let mut data = Extensions::new();
    /// data.insert("catalog".to_owned());
    /// assert_eq!(data.get::<String>().map(String::as_str), Some("catalog"));
    /// assert!(data.get::<&str>().is_none());
    /// ```
    #[must_use]
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<&T> {
        self.entries.get(&TypeId::of::<T>())?.value.downcast_ref()
    }

    /// Mutably borrows the value of the requested type, if present.
    ///
    /// # Example
    ///
    /// ```
    /// # use miju_core::task::extensions::Extensions;
    /// let mut data = Extensions::new();
    /// data.insert(vec!["first"]);
    /// data.get_mut::<Vec<&str>>().unwrap().push("second");
    /// assert_eq!(data.get::<Vec<&str>>().unwrap(), &["first", "second"]);
    /// ```
    pub fn get_mut<T: Send + Sync + 'static>(&mut self) -> Option<&mut T> {
        self.entries
            .get_mut(&TypeId::of::<T>())?
            .value
            .downcast_mut()
    }

    /// Removes and returns the value of the requested type.
    ///
    /// Ownership is transferred to the caller. Returns `None` when the type
    /// has no entry.
    ///
    /// # Example
    ///
    /// ```
    /// # use miju_core::task::extensions::Extensions;
    /// let mut data = Extensions::new();
    /// data.insert("catalog".to_owned());
    /// assert_eq!(data.remove::<String>(), Some("catalog".to_owned()));
    /// assert!(data.remove::<String>().is_none());
    /// assert!(data.is_empty());
    /// ```
    pub fn remove<T: Send + Sync + 'static>(&mut self) -> Option<T> {
        self.entries
            .remove(&TypeId::of::<T>())
            .map(Entry::into_value)
    }

    /// Returns the number of stored types.
    ///
    /// Replacing an existing entry does not change this count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no values are stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl fmt::Debug for Extensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Extensions")
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}
