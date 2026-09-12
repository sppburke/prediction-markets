use pe_source_core::SourceError;
use pe_source_polymarket_public::PageFetcher;
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Deterministic DESC venue, independent of the walker's cursor/window policy.
/// An explicit budget hangs before the next response, using the real timeout.
#[derive(Clone)]
pub struct HistoryFetcher {
    rows: Arc<Vec<Value>>,
    pub requests: Arc<Mutex<Vec<(i64, i64, usize)>>>,
    completed: Arc<AtomicUsize>,
    pub request_budget: usize,
    pub window_budget: usize,
}

impl HistoryFetcher {
    pub fn new(mut rows: Vec<Value>) -> Self {
        rows.sort_by_key(|row| std::cmp::Reverse(row["timestamp"].as_i64().unwrap()));
        Self {
            rows: Arc::new(rows),
            requests: Arc::default(),
            completed: Arc::default(),
            request_budget: usize::MAX,
            window_budget: usize::MAX,
        }
    }
}

impl PageFetcher for HistoryFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        let query: std::collections::HashMap<_, _> = url
            .split_once('?')
            .unwrap()
            .1
            .split('&')
            .map(|part| part.split_once('=').unwrap())
            .collect();
        assert_eq!(query["sortDirection"], "DESC");
        assert_eq!(query["limit"], "500");
        let start: i64 = query["start"].parse().unwrap();
        let end: i64 = query["end"].parse().unwrap();
        let offset: usize = query["offset"].parse().unwrap();
        assert!(start >= 1 && end >= start && offset <= 5000);
        let count = {
            let mut requests = self.requests.lock().unwrap();
            requests.push((start, end, offset));
            requests.len()
        };
        if count > self.request_budget
            || self.completed.load(Ordering::SeqCst) >= self.window_budget
        {
            return std::future::pending().await;
        }
        let lo = self
            .rows
            .partition_point(|r| r["timestamp"].as_i64().unwrap() > end);
        let hi = self
            .rows
            .partition_point(|r| r["timestamp"].as_i64().unwrap() >= start);
        let first = (lo + offset).min(hi);
        let last = (first + 500).min(hi);
        if last - first < 500 {
            self.completed.fetch_add(1, Ordering::SeqCst);
        }
        Ok(serde_json::to_vec(&self.rows[first..last]).unwrap())
    }
}
