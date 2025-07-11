use crate::airlock::types::OwnedReplicaAccountInfo;
use crossbeam_queue::ArrayQueue;
use log;
use once_cell::sync::Lazy;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

const POOL_SIZE: usize = 10000;

pub struct ObjectPool<T> {
    pool: ArrayQueue<T>,
    factory: Box<dyn Fn() -> T + Send + Sync>,
}

impl<T> ObjectPool<T> {
    pub fn new(capacity: usize, factory: impl Fn() -> T + Send + Sync + 'static) -> Self {
        let pool = ArrayQueue::new(capacity);

        for _ in 0..capacity.min(capacity / 2) {
            let _ = pool.push(factory());
        }

        Self {
            pool,
            factory: Box::new(factory),
        }
    }

    pub fn get(&self) -> T {
        self.pool.pop().unwrap_or_else(|| (self.factory)())
    }

    pub fn put(&self, item: T) {
        let _ = self.pool.push(item);
    }
}

pub struct PooledAccountInfo {
    inner: OwnedReplicaAccountInfo,
    pool: Arc<ObjectPool<OwnedReplicaAccountInfo>>,
}

impl PooledAccountInfo {
    pub fn new(pool: Arc<ObjectPool<OwnedReplicaAccountInfo>>) -> Self {
        Self {
            inner: pool.get(),
            pool,
        }
    }

    pub fn reset_and_populate(
        &mut self,
        info: &agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions,
        slot: u64,
        pubkey: &Pubkey,
    ) {
        use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoVersions;

        self.inner.pubkey = *pubkey;
        self.inner.slot = slot;

        match info {
            ReplicaAccountInfoVersions::V0_0_1(_) => {
                log::error!("Unsupported replica account info version V0_0_1. Only V0_0_3 and later are supported. Account: {}, Slot: {}", pubkey, slot);
                return; // Skip processing this account
            }
            ReplicaAccountInfoVersions::V0_0_2(_) => {
                log::error!("Unsupported replica account info version V0_0_2. Only V0_0_3 and later are supported. Account: {}, Slot: {}", pubkey, slot);
                return; // Skip processing this account
            }
            ReplicaAccountInfoVersions::V0_0_3(account_info) => {
                self.inner.lamports = account_info.lamports;
                self.inner.owner = Pubkey::try_from(account_info.owner).unwrap_or_default();
                self.inner.executable = account_info.executable;
                self.inner.rent_epoch = account_info.rent_epoch;
                self.inner.write_version = account_info.write_version;
                self.inner.txn_signature = None; // V3 doesn't have txn_signature

                self.inner.data.clear();
                self.inner.data.extend_from_slice(account_info.data);
            }
        }
    }

    pub fn into_inner(self) -> OwnedReplicaAccountInfo {
        self.inner.clone()
    }
}

impl Drop for PooledAccountInfo {
    fn drop(&mut self) {
        let mut recycled = self.inner.clone();
        recycled.data.clear();
        //recycled.data.shrink_to(1024);
        self.pool.put(recycled);
    }
}

static GLOBAL_ACCOUNT_POOL: Lazy<Arc<ObjectPool<OwnedReplicaAccountInfo>>> = Lazy::new(|| {
    Arc::new(ObjectPool::new(POOL_SIZE, || OwnedReplicaAccountInfo {
        pubkey: Pubkey::default(),
        lamports: 0,
        owner: Pubkey::default(),
        executable: false,
        rent_epoch: 0,
        data: Vec::with_capacity(1024),
        write_version: 0,
        slot: 0,
        txn_signature: None,
    }))
});

#[macro_export]
macro_rules! get_owned_account_info_from_pool {
    ($replica_info:ident, $slot:expr, $pubkey:expr) => {{
        let pool = crate::airlock::pool::get_global_pool();
        let mut pooled = crate::airlock::pool::PooledAccountInfo::new(pool);
        pooled.reset_and_populate($replica_info, $slot, $pubkey);
        pooled.into_inner()
    }};
}

pub fn get_global_pool() -> Arc<ObjectPool<OwnedReplicaAccountInfo>> {
    GLOBAL_ACCOUNT_POOL.clone()
}
