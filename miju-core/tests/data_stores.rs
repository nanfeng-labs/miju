//! Typed data isolation and metadata update behavior.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use miju_core::task::{
    data::MissingDataError,
    extensions::Extensions,
    metadata::{MetadataError, MetadataStore},
};

#[test]
fn extensions_checked_lookup_and_lazy_insert_merge_by_type() {
    let mut data = Extensions::new();
    assert_eq!(
        data.get_checked::<u32>(),
        Err(MissingDataError::NotFound("u32".to_owned()))
    );
    assert_eq!(
        data.get_checked::<u32>().unwrap_err().to_string(),
        "the type for key `u32` is not available"
    );
    *data.get_or_insert(4_u32) += 1;
    assert_eq!(
        *data.get_or_insert_with::<u32, _>(|| panic!("already present")),
        5
    );
    let mut other = Extensions::new();
    other.insert(9_u32);
    other.insert("other");
    data.extend(other.clone());
    assert_eq!(data.get_checked::<u32>(), Ok(&9));
    assert_eq!(data.get::<&str>(), Some(&"other"));
    let mut empty = Extensions::new();
    empty.extend(other);
    empty.extend(Extensions::new());
    assert_eq!(empty.len(), 2);
    empty.clear();
    assert!(empty.is_empty());
}

#[test]
fn extensions_replace_by_exact_type_and_remove_owned_values() {
    let mut data = Extensions::new();
    assert!(data.get::<u32>().is_none());
    assert!(data.get_mut::<String>().is_none());
    assert!(data.remove::<String>().is_none());
    assert_eq!(data.insert(12_u32), None);
    assert_eq!(data.insert(12_u64), None);
    assert_eq!(data.insert(30_u32), Some(12));
    assert_eq!(data.len(), 2);
    *data.get_mut::<u64>().unwrap() += 1;
    assert_eq!(data.remove::<u64>(), Some(13));
    assert_eq!(data.get::<u32>(), Some(&30));
    assert_eq!(data.remove::<u32>(), Some(30));
    assert!(data.is_empty());
}

#[test]
fn extension_clones_follow_each_values_clone_semantics() {
    let mut original = Extensions::new();
    original.insert(vec![1_u32, 2]);
    let shared = Arc::new(AtomicUsize::new(0));
    original.insert(Arc::clone(&shared));
    let mut copy = original.clone();
    copy.get_mut::<Vec<u32>>().unwrap().push(3);
    let mut third = copy.clone();
    third.get_mut::<Vec<u32>>().unwrap().push(4);
    assert_eq!(original.get::<Vec<u32>>().unwrap(), &[1, 2]);
    assert_eq!(copy.get::<Vec<u32>>().unwrap(), &[1, 2, 3]);
    assert_eq!(third.get::<Vec<u32>>().unwrap(), &[1, 2, 3, 4]);
    copy.get::<Arc<AtomicUsize>>()
        .unwrap()
        .store(5, Ordering::SeqCst);
    assert_eq!(shared.load(Ordering::SeqCst), 5);
    assert!(Arc::ptr_eq(
        original.get::<Arc<AtomicUsize>>().unwrap(),
        third.get::<Arc<AtomicUsize>>().unwrap()
    ));
}

#[test]
fn metadata_duplicates_preserve_values_and_explicit_replacement_returns_them() {
    let mut metadata = MetadataStore::new();
    metadata.insert("region", "north").unwrap();
    assert_eq!(
        metadata.insert("region", "south"),
        Err(MetadataError::DuplicateKey("region".to_owned()))
    );
    assert_eq!(metadata.get("region"), Some("north"));
    assert_eq!(metadata.replace("region", "east"), Some("north".to_owned()));
    assert_eq!(metadata.remove("region"), Some("east".to_owned()));
    assert_eq!(metadata.remove("region"), None);
    assert!(metadata.is_empty());
}

#[test]
fn metadata_extend_overwrites_in_order_without_affecting_a_clone() {
    let mut metadata = MetadataStore::new();
    metadata.insert("route", "original").unwrap();
    let original = metadata.clone();
    metadata.extend([("route", "first"), ("route", "last"), ("", "")]);
    assert_eq!(metadata.get("route"), Some("last"));
    assert_eq!(original.get("route"), Some("original"));
    assert_eq!(metadata.get(""), Some(""));
    assert_eq!(metadata.len(), 2);
    let entries: std::collections::HashMap<_, _> = metadata.iter().collect();
    assert_eq!(
        entries
            .get(&"route".to_owned())
            .copied()
            .map(String::as_str),
        Some("last")
    );
}

#[test]
fn metadata_container_operations_keep_their_distinct_write_rules() {
    let mut metadata: MetadataStore = [("key", "first"), ("key", "last")].into_iter().collect();
    assert_eq!(metadata["key"], "last");
    assert!(!metadata.insert_if_absent("key", "ignored"));
    assert!(metadata.insert_if_absent("new", "value"));
    assert_eq!(metadata.replace("absent", "created"), None);
    metadata["key"].push('!');
    metadata.get_mut("new").unwrap().push('!');
    for (_, value) in &mut metadata {
        value.push('?');
    }
    for value in metadata.values_mut() {
        value.push('.');
    }
    assert_eq!(metadata.get("key"), Some("last!?."));
    assert_eq!(metadata.keys().count(), 3);
    assert_eq!(metadata.values().count(), 3);
    assert_eq!(metadata.iter_mut().count(), 3);
    assert_eq!((&metadata).into_iter().count(), 3);
    let copy = MetadataStore::from_map(metadata.as_map().clone());
    assert_eq!(copy.as_ref(), metadata.as_map());
    let inner = copy.into_inner();
    assert_eq!(inner.len(), 3);
    metadata
        .as_mut()
        .insert("raw".to_owned(), "entry".to_owned());
    assert_eq!(metadata.clone().into_iter().count(), 4);
    metadata.clear();
    assert!(metadata.is_empty());
    assert_eq!(
        MetadataError::DuplicateKey("key".to_owned()).to_string(),
        "The key already exists in the store"
    );
}

#[test]
#[should_panic(expected = "metadata key `missing` does not exist")]
fn indexing_missing_metadata_mutably_panics() {
    MetadataStore::new()["missing"].push('!');
}
