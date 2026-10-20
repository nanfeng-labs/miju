//! Typed data isolation and metadata update behavior.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use miju_core::task::{
    extensions::Extensions,
    metadata::{MetadataError, MetadataStore},
};

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
    assert_eq!(entries.get("route"), Some(&"last"));
}
