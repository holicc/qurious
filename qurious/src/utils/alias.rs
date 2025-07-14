use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct AliasGenerator {
    next_id: AtomicU64,
}

impl AliasGenerator {
    pub fn next(&self, prefix: &str) -> String {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        format!("{prefix}_{id}")
    }
}
