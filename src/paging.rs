//! Cursor paging decisions, kept pure so they can be tested without a cluster.

use serde_json::Value;

/// Number of documents to request in the next page, never overshooting `target`.
///
/// `target` is the live document count, or `--limit` when that is lower.
pub fn page_size(batch_size: u16, fetched: u32, target: u32) -> u16 {
    let remaining = target.saturating_sub(fetched);
    (batch_size as u32).min(remaining) as u16
}

/// Cursor for the next page, or `None` when the index is exhausted.
///
/// A page holding fewer hits than were requested means there is nothing after it. An empty
/// page is the same signal, and is also the only case where `last_sort` is absent.
///
/// The cursor is always the `sort` value Elasticsearch returned for the last hit. It must
/// never be derived from `fetched`: sort values are internal Lucene document ids, which skip
/// every id held by a tombstoned document, so a count-derived cursor points at an earlier
/// document than intended and the walk never reaches the end of the index.
pub fn next_cursor(
    hits: usize,
    requested: u16,
    last_sort: Option<Vec<Value>>,
) -> Option<Vec<Value>> {
    match last_sort {
        // `hits > 0` is not redundant with `hits == requested`: a zero-size request would
        // otherwise report itself as a full page and the walk would never terminate.
        Some(cursor) if hits > 0 && hits == requested as usize => Some(cursor),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cursor(v: i64) -> Option<Vec<Value>> {
        Some(vec![json!(v)])
    }

    #[test]
    fn page_size_is_the_batch_size_while_far_from_the_target() {
        assert_eq!(page_size(1000, 0, 8282), 1000);
        assert_eq!(page_size(1000, 7000, 8282), 1000);
    }

    #[test]
    fn page_size_shrinks_to_the_remainder_near_the_target() {
        assert_eq!(page_size(1000, 8000, 8282), 282);
        assert_eq!(page_size(10_000, 0, 169), 169);
    }

    #[test]
    fn page_size_is_zero_once_the_target_is_reached_or_passed() {
        assert_eq!(page_size(1000, 8282, 8282), 0);
        assert_eq!(page_size(1000, 9000, 8282), 0);
        assert_eq!(page_size(1000, 0, 0), 0);
    }

    #[test]
    fn a_full_page_yields_the_cursor_it_returned() {
        assert_eq!(next_cursor(1000, 1000, cursor(4711)), cursor(4711));
    }

    #[test]
    fn a_short_page_ends_the_walk() {
        assert_eq!(next_cursor(282, 1000, cursor(8281)), None);
    }

    #[test]
    fn an_empty_page_ends_the_walk() {
        assert_eq!(next_cursor(0, 1000, None), None);
        // Defensive: a cursor alongside zero hits is still the end.
        assert_eq!(next_cursor(0, 1000, cursor(1)), None);
    }

    #[test]
    fn a_multi_value_cursor_is_carried_through_verbatim() {
        // Sorting by `_doc` under a point-in-time makes Elasticsearch append an implicit
        // `_shard_doc` tiebreaker, so a page's sort values can be wider than one element.
        // Whatever arrives must go back out unchanged.
        let wide = Some(vec![json!(50), json!(50)]);
        assert_eq!(next_cursor(1000, 1000, wide.clone()), wide);
    }

    /// Walk an index the way the driver does, and report (pages, documents fetched).
    ///
    /// `live` is what Elasticsearch will actually hand over; `target` is what the count said.
    /// They differ when a `--query` filter is set, since the walk then ends early.
    fn walk(target: u32, live: u32, batch: u16) -> (u32, u32) {
        let mut fetched = 0u32;
        let mut pages = 0u32;
        while fetched < target {
            let size = page_size(batch, fetched, target);
            // Elasticsearch fills each page until the matching documents run out.
            let hits = (size as u32).min(live - fetched) as usize;
            fetched += hits as u32;
            pages += 1;
            assert!(pages < 10_000, "walk failed to terminate");
            if next_cursor(hits, size, cursor(fetched as i64)).is_none() {
                break;
            }
        }
        (pages, fetched)
    }

    #[test]
    fn walking_an_index_with_tombstones_terminates_on_the_short_page() {
        // 8282 live documents in pages of 1000: eight full pages, then a 282-hit page.
        // The index's 3494 tombstones are invisible here precisely because the cursor comes
        // from Elasticsearch rather than from `fetched` — which is what v0.1.1 got wrong.
        assert_eq!(walk(8282, 8282, 1000), (9, 8282));
    }

    #[test]
    fn walking_terminates_when_the_page_size_divides_the_count_exactly() {
        // The last full page is followed by one empty page, which ends the walk.
        assert_eq!(walk(4000, 4000, 1000), (4, 4000));
    }

    #[test]
    fn a_single_page_larger_than_the_index_is_one_page() {
        assert_eq!(walk(169, 169, 10_000), (1, 169));
    }

    #[test]
    fn an_empty_index_is_never_requested() {
        assert_eq!(walk(0, 0, 1000), (0, 0));
    }

    #[test]
    fn a_filter_matching_fewer_documents_than_the_count_still_terminates() {
        // Belt and braces: --query narrows the result set, so the walk ends on a short page
        // well before `target`.
        assert_eq!(walk(8282, 35, 1000), (1, 35));
    }
}
