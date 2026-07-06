//! The polling loop every integration binary shares. Budgets differ
//! per binary (in-memory convergence versus real compaction versus
//! slow-CI soaks), so each wraps this with its own.

use std::time::Duration;

/// Poll an async condition every 100ms until it yields, panicking
/// when `budget` runs out.
pub async fn poll_until_within<T, F, Fut>(what: &str, budget: Duration, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for: {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
