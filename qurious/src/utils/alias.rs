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

/// The alias generators shared across nested subquery optimization.
///
/// Each nesting level must keep generating from the same counters: a level that restarts at 0 can
/// hand an inner subquery the same alias as the one it is nested inside, which makes column
/// references ambiguous.
#[derive(Default, Clone)]
pub struct SubqueryAliases {
    pub scalar: std::sync::Arc<AliasGenerator>,
    pub predicate: std::sync::Arc<AliasGenerator>,
}
