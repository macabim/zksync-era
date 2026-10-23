use std::time::{Duration, Instant};

use futures::{stream::FuturesUnordered, StreamExt};
use zksync_dal::{ConnectionPool, Core, CoreDal};
use zksync_types::H256;

const MAX_KEYS: usize = 16_384;
const MIN_KEYS: usize = 512;
const CONCURRENCY: usize = 4;

/// Bounds optional cache warming so it cannot hold up sealing during database trouble.
pub(super) async fn prefetch(pool: &ConnectionPool<Core>, keys: &[H256]) {
    if keys.len() < MIN_KEYS || pool.max_size() <= 1 {
        return;
    }
    let started = Instant::now();
    let keys = &keys[..keys.len().min(MAX_KEYS)];
    let concurrency = CONCURRENCY.min((pool.max_size() - 1) as usize);
    let chunks = keys.chunks(keys.len().div_ceil(concurrency));
    let probes = FuturesUnordered::new();
    for chunk in chunks {
        probes.push(probe_chunk(pool, chunk));
    }
    let probes = probes.collect::<Vec<_>>();
    // Scoped futures leave no detached tasks. Each SQL query also has a server deadline.
    let result = tokio::time::timeout(Duration::from_millis(600), probes).await;
    tracing::info!(
        keys = keys.len(),
        elapsed_ms = started.elapsed().as_millis(),
        completed = result
            .as_ref()
            .map_or(0, |probes| probes.iter().filter(|ok| **ok).count()),
        timed_out = result.is_err(),
        "Prefetched initial write index pages"
    );
}

async fn probe_chunk(pool: &ConnectionPool<Core>, keys: &[H256]) -> bool {
    let Ok(Ok(mut connection)) = tokio::time::timeout(
        Duration::from_millis(20),
        pool.connection_tagged("initial_write_prefetch"),
    )
    .await
    else {
        return false;
    };
    match connection
        .storage_logs_dedup_dal()
        .prefetch_initial_write_keys(keys)
        .await
    {
        Ok(()) => true,
        Err(err) => {
            tracing::debug!(%err, "Skipping initial write index prefetch");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn prefetch_skips_empty_keys_and_a_single_connection_pool() {
        let test_pool = ConnectionPool::<Core>::test_pool().await;
        let pool = ConnectionPool::<Core>::builder(test_pool.database_url().clone(), 1)
            .build()
            .await
            .unwrap();
        let _reserved = pool.connection().await.unwrap();
        tokio::time::timeout(
            Duration::from_millis(100),
            prefetch(&pool, &vec![H256::zero(); 1024]),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_millis(100), prefetch(&pool, &[]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn prefetch_does_not_wait_for_an_exhausted_pool() {
        let test_pool = ConnectionPool::<Core>::test_pool().await;
        let pool = ConnectionPool::<Core>::builder(test_pool.database_url().clone(), 2)
            .build()
            .await
            .unwrap();
        let _first = pool.connection().await.unwrap();
        let _second = pool.connection().await.unwrap();
        tokio::time::timeout(
            Duration::from_millis(200),
            prefetch(&pool, &vec![H256::zero(); 1024]),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn prefetch_keeps_initial_write_indices_unchanged() {
        let pool = ConnectionPool::<Core>::test_pool().await;
        let keys: Vec<_> = (0..1024).rev().map(H256::from_low_u64_be).collect();
        let mut conn = pool.connection().await.unwrap();
        prefetch(&pool, &keys).await;
        conn.storage_logs_dedup_dal()
            .insert_initial_writes(zksync_types::L1BatchNumber(0), &keys)
            .await
            .unwrap();
        let mut writes = conn
            .storage_logs_dedup_dal()
            .dump_all_initial_writes_for_tests()
            .await;
        writes.sort_by_key(|write| write.index);
        assert_eq!(writes.len(), keys.len());
        for (i, write) in writes.iter().enumerate() {
            assert_eq!(write.hashed_key, keys[i]);
            assert_eq!(write.index, i as u64 + 1);
        }
    }
}
