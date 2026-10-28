//! Exact pagination offsets and serialized filter values.

use std::panic::catch_unwind;

use miju_core::backend::Filter;

#[test]
fn ordinary_page_offsets_and_limits_are_preserved() {
    for (page, page_size, expected_offset, expected_limit) in [
        (1, None, 0, 10),
        (2, None, 10, 10),
        (2, Some(u32::MAX), 4_294_967_295, u32::MAX),
        (65_536, Some(65_536), 4_294_901_760, 65_536),
        (u32::MAX, Some(0), 0, 0),
    ] {
        let filter = Filter {
            status: None,
            page,
            page_size,
        };
        assert_eq!(u128::from(filter.offset()), expected_offset);
        assert_eq!(filter.limit(), expected_limit);
    }
}

#[test]
fn positive_page_offset_can_exceed_u32() {
    let filter = Filter {
        status: None,
        page: 65_537,
        page_size: Some(65_536),
    };
    let offset = catch_unwind(|| filter.offset());
    assert!(offset.is_ok(), "a positive page offset must not panic");
    assert_eq!(u128::from(offset.unwrap()), 4_294_967_296);
}

#[test]
fn largest_page_and_size_keep_the_exact_offset() {
    let filter = Filter {
        status: None,
        page: u32::MAX,
        page_size: Some(u32::MAX),
    };
    let offset = catch_unwind(|| filter.offset());
    assert!(offset.is_ok(), "a positive page offset must not panic");
    assert_eq!(u128::from(offset.unwrap()), 18_446_744_060_824_649_730);
}

#[cfg(feature = "serde")]
#[test]
fn serialized_filters_preserve_defaults_and_large_page_values() {
    let default: Filter = serde_json::from_str("{}").unwrap();
    assert_eq!(default.page, 1);
    assert_eq!(default.page_size, None);
    assert_eq!(default.offset(), 0);
    assert_eq!(default.limit(), 10);

    let input = r#"{"status":null,"page":65537,"page_size":65536}"#;
    let filter: Filter = serde_json::from_str(input).unwrap();
    assert_eq!(serde_json::to_string(&filter).unwrap(), input);
    let offset = catch_unwind(|| filter.offset());
    assert!(
        offset.is_ok(),
        "a decoded positive page offset must not panic"
    );
    assert_eq!(u128::from(offset.unwrap()), 4_294_967_296);
    assert_eq!(filter.limit(), 65_536);
}
