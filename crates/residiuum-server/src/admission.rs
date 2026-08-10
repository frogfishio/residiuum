//! Protocol admission control (DEF-034).
//!
//! Bounds abuse and overload at the RPC edge, complementary to connection
//! slots ([`crate::runtime`]) and host resource ceilings ([`residiuum_sdk::resource`]):
//!
//! - **Global and per-principal rate limits** on application RPCs
//! - **Authentication failure budgets** with temporary lockout (no secret storage)
//! - **Connection churn** limits at accept time
//! - **Expensive-op concurrency budgets** (scan / find / index rebuild / salvage…)
//! - **Operation-id replay window** for idempotent mutation retries
//!
//! Overload answers use [`residiuum_sdk::ErrorCode::ResourceLimit`] (`resource_limit`).
//! Auth lockouts use [`residiuum_sdk::ErrorCode::AuthenticationFailed`] with a generic
//! message so timing/content cannot distinguish "bad token" from "locked out".
//!
//! Profile tag: [`ADMISSION_PROFILE`].

use residiuum_sdk::Error;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Admission-control profile label (capability matrices / startup logs).
pub const ADMISSION_PROFILE: &str = "residiuum-admission-v1";

/// Default global RPC rate (requests per second, process-wide).
pub const DEFAULT_GLOBAL_MAX_RPS: u32 = 10_000;

/// Default per-principal RPC rate (requests per second).
pub const DEFAULT_PER_PRINCIPAL_MAX_RPS: u32 = 2_000;

/// Default auth-failure count before lockout (per failure key, per window).
pub const DEFAULT_MAX_AUTH_FAILURES: u32 = 20;

/// Default sliding window for counting auth failures.
pub const DEFAULT_AUTH_FAILURE_WINDOW: Duration = Duration::from_secs(60);

/// Default lockout duration after the auth-failure budget is exhausted.
pub const DEFAULT_AUTH_LOCKOUT: Duration = Duration::from_secs(30);

/// Default maximum new TCP connections admitted per connect window.
pub const DEFAULT_MAX_CONNECTS_PER_WINDOW: u32 = 500;

/// Default window for connection-churn accounting.
pub const DEFAULT_CONNECT_WINDOW: Duration = Duration::from_secs(10);

/// Default maximum concurrent expensive RPCs (scan/find/index/salvage…).
pub const DEFAULT_MAX_EXPENSIVE_CONCURRENT: usize = 8;

/// Default capacity of the operation-id replay window.
pub const DEFAULT_REPLAY_CAPACITY: usize = 4_096;

/// Default TTL for operation-id replay entries.
pub const DEFAULT_REPLAY_TTL: Duration = Duration::from_secs(300);

/// Fixed rate-limit window length (1 second buckets).
pub const RATE_WINDOW: Duration = Duration::from_secs(1);

/// Operator-facing admission limits for [`crate::ServeOptions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Process-wide application RPCs allowed per second.
    pub global_max_rps: u32,
    /// Per authenticated principal RPCs allowed per second.
    pub per_principal_max_rps: u32,
    /// Failed authentications allowed per key within [`Self::auth_failure_window`].
    pub max_auth_failures: u32,
    /// Sliding window for auth-failure counting.
    pub auth_failure_window: Duration,
    /// Lockout after the failure budget is exhausted.
    pub auth_lockout: Duration,
    /// New connections admitted per [`Self::connect_window`].
    pub max_connects_per_window: u32,
    /// Window for connection-churn accounting.
    pub connect_window: Duration,
    /// Concurrent expensive operations (scan/find/index rebuild/…).
    pub max_expensive_concurrent: usize,
    /// Bound on remembered operation ids for idempotent retries.
    pub replay_capacity: usize,
    /// How long an operation id stays in the replay window.
    pub replay_ttl: Duration,
}

impl Default for AdmissionLimits {
    fn default() -> Self {
        Self {
            global_max_rps: DEFAULT_GLOBAL_MAX_RPS,
            per_principal_max_rps: DEFAULT_PER_PRINCIPAL_MAX_RPS,
            max_auth_failures: DEFAULT_MAX_AUTH_FAILURES,
            auth_failure_window: DEFAULT_AUTH_FAILURE_WINDOW,
            auth_lockout: DEFAULT_AUTH_LOCKOUT,
            max_connects_per_window: DEFAULT_MAX_CONNECTS_PER_WINDOW,
            connect_window: DEFAULT_CONNECT_WINDOW,
            max_expensive_concurrent: DEFAULT_MAX_EXPENSIVE_CONCURRENT,
            replay_capacity: DEFAULT_REPLAY_CAPACITY,
            replay_ttl: DEFAULT_REPLAY_TTL,
        }
    }
}

impl AdmissionLimits {
    /// Draft defaults used by `residiuum serve` and library helpers.
    pub fn draft_defaults() -> Self {
        Self::default()
    }

    /// Clamp zero/overflow into safe minimums.
    pub fn normalized(self) -> Self {
        Self {
            global_max_rps: self.global_max_rps.max(1),
            per_principal_max_rps: self.per_principal_max_rps.max(1),
            max_auth_failures: self.max_auth_failures.max(1),
            auth_failure_window: if self.auth_failure_window.is_zero() {
                DEFAULT_AUTH_FAILURE_WINDOW
            } else {
                self.auth_failure_window
            },
            auth_lockout: self.auth_lockout,
            max_connects_per_window: self.max_connects_per_window.max(1),
            connect_window: if self.connect_window.is_zero() {
                DEFAULT_CONNECT_WINDOW
            } else {
                self.connect_window
            },
            max_expensive_concurrent: self.max_expensive_concurrent.max(1),
            replay_capacity: self.replay_capacity.max(1),
            replay_ttl: if self.replay_ttl.is_zero() {
                DEFAULT_REPLAY_TTL
            } else {
                self.replay_ttl
            },
        }
    }

    /// Tight limits useful for unit and integration abuse tests.
    pub fn for_tests() -> Self {
        Self {
            global_max_rps: 1_000,
            per_principal_max_rps: 1_000,
            max_auth_failures: 3,
            auth_failure_window: Duration::from_secs(60),
            auth_lockout: Duration::from_secs(30),
            max_connects_per_window: 1_000,
            connect_window: Duration::from_secs(10),
            max_expensive_concurrent: 2,
            replay_capacity: 64,
            replay_ttl: Duration::from_secs(60),
        }
    }
}

/// Whether an operation is treated as expensive for concurrency budgets.
pub fn is_expensive_op(op: &str) -> bool {
    matches!(
        op,
        "scan_json"
            | "find"
            | "history"
            | "index_rebuild"
            | "index_create"
            | "salvage_export"
            | "admin_stats"
            | "tier_move"
            | "purge"
            | "force_reconfig"
            | "get_payload"
    )
}

/// Whether the op is a mutation that may present an operation id for replay.
pub fn is_replayable_mutation(op: &str) -> bool {
    matches!(op, "put" | "put_bytes" | "delete")
}

/// Outcome of registering an operation id in the replay window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayStatus {
    /// First time this (principal, operation_id) was seen in the window.
    Fresh,
    /// Idempotent retry of a known operation id within TTL.
    Retry,
}

/// Snapshot of admission counters for diagnostics and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AdmissionStats {
    /// Application RPCs admitted under rate limits.
    pub admitted_rpcs: u64,
    /// RPCs rejected by global or per-principal rate.
    pub rate_rejected: u64,
    /// Auth attempts rejected because the failure key is locked out.
    pub auth_lockouts: u64,
    /// Auth failures recorded (before lockout).
    pub auth_failures: u64,
    /// Connections rejected by churn limit.
    pub connect_churn_rejected: u64,
    /// Connections admitted under churn accounting.
    pub connects_admitted: u64,
    /// Expensive ops rejected because the concurrency budget was full.
    pub expensive_rejected: u64,
    /// Expensive ops that entered the budget.
    pub expensive_started: u64,
    /// Expensive ops that left the budget.
    pub expensive_finished: u64,
    /// Fresh operation ids registered.
    pub replay_fresh: u64,
    /// Idempotent retries observed in the replay window.
    pub replay_retries: u64,
    /// New operation ids rejected because the replay window was full.
    pub replay_rejected: u64,
}

/// Shared process-wide admission controller (DEF-034).
///
/// Safe to share across accept loop and connection workers via [`Arc`].
#[derive(Debug)]
pub struct AdmissionController {
    limits: AdmissionLimits,
    inner: Mutex<AdmissionInner>,
    admitted_rpcs: AtomicU64,
    rate_rejected: AtomicU64,
    auth_lockouts: AtomicU64,
    auth_failures: AtomicU64,
    connect_churn_rejected: AtomicU64,
    connects_admitted: AtomicU64,
    expensive_rejected: AtomicU64,
    expensive_started: AtomicU64,
    expensive_finished: AtomicU64,
    replay_fresh: AtomicU64,
    replay_retries: AtomicU64,
    replay_rejected: AtomicU64,
}

#[derive(Debug)]
struct AdmissionInner {
    global: FixedWindow,
    principals: HashMap<String, FixedWindow>,
    auth: HashMap<u64, AuthFailureState>,
    connects: FixedWindow,
    expensive_active: usize,
    /// FIFO of (insert_time, principal_id, op_id) for TTL eviction.
    replay_order: VecDeque<(Instant, String, String)>,
    /// Set membership: principal\0op_id → insert Instant.
    replay_index: HashMap<String, Instant>,
}

#[derive(Debug, Clone)]
struct FixedWindow {
    window_start: Instant,
    count: u32,
    limit: u32,
    window: Duration,
}

impl FixedWindow {
    fn new(limit: u32, window: Duration) -> Self {
        Self {
            window_start: Instant::now(),
            count: 0,
            limit: limit.max(1),
            window,
        }
    }

    fn reset_if_elapsed(&mut self, now: Instant) {
        if now.duration_since(self.window_start) >= self.window {
            self.window_start = now;
            self.count = 0;
        }
    }

    /// Try to consume one unit; returns false when the limit is exhausted.
    fn try_take(&mut self, now: Instant) -> bool {
        self.reset_if_elapsed(now);
        if self.count >= self.limit {
            return false;
        }
        self.count = self.count.saturating_add(1);
        true
    }
}

#[derive(Debug, Clone)]
struct AuthFailureState {
    window_start: Instant,
    failures: u32,
    lockout_until: Option<Instant>,
}

impl AdmissionController {
    /// Create a controller with the given limits.
    pub fn new(limits: AdmissionLimits) -> Arc<Self> {
        let limits = limits.normalized();
        Arc::new(Self {
            inner: Mutex::new(AdmissionInner {
                global: FixedWindow::new(limits.global_max_rps, RATE_WINDOW),
                principals: HashMap::new(),
                auth: HashMap::new(),
                connects: FixedWindow::new(limits.max_connects_per_window, limits.connect_window),
                expensive_active: 0,
                replay_order: VecDeque::new(),
                replay_index: HashMap::new(),
            }),
            limits,
            admitted_rpcs: AtomicU64::new(0),
            rate_rejected: AtomicU64::new(0),
            auth_lockouts: AtomicU64::new(0),
            auth_failures: AtomicU64::new(0),
            connect_churn_rejected: AtomicU64::new(0),
            connects_admitted: AtomicU64::new(0),
            expensive_rejected: AtomicU64::new(0),
            expensive_started: AtomicU64::new(0),
            expensive_finished: AtomicU64::new(0),
            replay_fresh: AtomicU64::new(0),
            replay_retries: AtomicU64::new(0),
            replay_rejected: AtomicU64::new(0),
        })
    }

    /// Limits in effect.
    pub fn limits(&self) -> &AdmissionLimits {
        &self.limits
    }

    /// Snapshot counters.
    pub fn stats(&self) -> AdmissionStats {
        AdmissionStats {
            admitted_rpcs: self.admitted_rpcs.load(Ordering::Relaxed),
            rate_rejected: self.rate_rejected.load(Ordering::Relaxed),
            auth_lockouts: self.auth_lockouts.load(Ordering::Relaxed),
            auth_failures: self.auth_failures.load(Ordering::Relaxed),
            connect_churn_rejected: self.connect_churn_rejected.load(Ordering::Relaxed),
            connects_admitted: self.connects_admitted.load(Ordering::Relaxed),
            expensive_rejected: self.expensive_rejected.load(Ordering::Relaxed),
            expensive_started: self.expensive_started.load(Ordering::Relaxed),
            expensive_finished: self.expensive_finished.load(Ordering::Relaxed),
            replay_fresh: self.replay_fresh.load(Ordering::Relaxed),
            replay_retries: self.replay_retries.load(Ordering::Relaxed),
            replay_rejected: self.replay_rejected.load(Ordering::Relaxed),
        }
    }

    /// Admit one new TCP connection under the churn window.
    ///
    /// Returns `false` when the connect rate is exhausted. Callers still apply
    /// [`crate::runtime::ServerRuntime::try_admit_connection`] for simultaneous caps.
    pub fn try_admit_connect(&self) -> bool {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if g.connects.try_take(now) {
            self.connects_admitted.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            self.connect_churn_rejected.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Stable non-secret key for auth-failure accounting.
    ///
    /// Hashes the presented token (or a fixed empty marker) so raw secrets are
    /// never stored in the failure map.
    pub fn auth_failure_key(presented: Option<&str>) -> u64 {
        let bytes = presented.unwrap_or("").as_bytes();
        // Two-pass FNV-ish mix; not a MAC — only a map key.
        let mut h: u64 = 0xcbf29ce484222325;
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x100000001b3);
        }
        // Mix length so empty and short tokens spread differently.
        h ^= (bytes.len() as u64).wrapping_mul(0x9e3779b97f4a7c15);
        h
    }

    /// Return an error when this failure key is currently locked out.
    pub fn check_auth_lockout(&self, presented: Option<&str>) -> Result<(), Error> {
        let key = Self::auth_failure_key(presented);
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if let Some(state) = g.auth.get_mut(&key) {
            if let Some(until) = state.lockout_until {
                if now < until {
                    self.auth_lockouts.fetch_add(1, Ordering::Relaxed);
                    return Err(Error::AuthenticationFailed(
                        "too many authentication failures; try again later".into(),
                    ));
                }
                // Lockout expired: clear so a fresh window can begin.
                state.lockout_until = None;
                state.failures = 0;
                state.window_start = now;
            }
        }
        Ok(())
    }

    /// Record an authentication failure; may enter lockout.
    pub fn record_auth_failure(&self, presented: Option<&str>) {
        let key = Self::auth_failure_key(presented);
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let window = self.limits.auth_failure_window;
        let max = self.limits.max_auth_failures;
        let lockout = self.limits.auth_lockout;
        let state = g.auth.entry(key).or_insert_with(|| AuthFailureState {
            window_start: now,
            failures: 0,
            lockout_until: None,
        });
        if let Some(until) = state.lockout_until {
            if now < until {
                return;
            }
            state.lockout_until = None;
            state.failures = 0;
            state.window_start = now;
        }
        if now.duration_since(state.window_start) >= window {
            state.window_start = now;
            state.failures = 0;
        }
        state.failures = state.failures.saturating_add(1);
        self.auth_failures.fetch_add(1, Ordering::Relaxed);
        if state.failures >= max && !lockout.is_zero() {
            state.lockout_until = Some(now + lockout);
        }
        // Bound map growth under random-token spray.
        if g.auth.len() > 10_000 {
            g.auth.retain(|_, s| {
                s.lockout_until.map(|u| u > now).unwrap_or(false)
                    || now.duration_since(s.window_start) < window
            });
        }
    }

    /// Clear failure state after a successful authentication (optional hygiene).
    pub fn clear_auth_failures(&self, presented: Option<&str>) {
        let key = Self::auth_failure_key(presented);
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.auth.remove(&key);
    }

    /// Admit one application RPC under global + per-principal rate limits.
    ///
    /// `principal_id` should be the authenticated public id (never a token).
    pub fn admit_rpc(&self, principal_id: &str) -> Result<(), Error> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if !g.global.try_take(now) {
            self.rate_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(Error::ResourceLimit(format!(
                "global RPC rate limit exceeded (max {}/s)",
                self.limits.global_max_rps
            )));
        }
        let pid = bound_principal(principal_id);
        let limit = self.limits.per_principal_max_rps;
        let entry = g
            .principals
            .entry(pid)
            .or_insert_with(|| FixedWindow::new(limit, RATE_WINDOW));
        // Keep limit in sync if ServeOptions rebuilt with new limits (same Arc).
        entry.limit = limit;
        if !entry.try_take(now) {
            self.rate_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(Error::ResourceLimit(format!(
                "per-principal RPC rate limit exceeded (max {}/s)",
                self.limits.per_principal_max_rps
            )));
        }
        // Bound principal map growth.
        if g.principals.len() > 10_000 {
            g.principals
                .retain(|_, w| now.duration_since(w.window_start) < RATE_WINDOW.saturating_mul(2));
        }
        self.admitted_rpcs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Enter the expensive-op concurrency budget; guard releases the slot on drop.
    ///
    /// Cheap ops return a no-op guard. Call on the shared [`Arc`] from the serve path.
    pub fn try_begin_expensive(self: &Arc<Self>, op: &str) -> Result<ExpensiveGuard, Error> {
        if !is_expensive_op(op) {
            return Ok(ExpensiveGuard { controller: None });
        }
        {
            let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if g.expensive_active >= self.limits.max_expensive_concurrent {
                self.expensive_rejected.fetch_add(1, Ordering::Relaxed);
                return Err(Error::ResourceLimit(format!(
                    "expensive operation concurrency limit exceeded (max {})",
                    self.limits.max_expensive_concurrent
                )));
            }
            g.expensive_active += 1;
        }
        self.expensive_started.fetch_add(1, Ordering::Relaxed);
        Ok(ExpensiveGuard {
            controller: Some(Arc::clone(self)),
        })
    }

    fn end_expensive(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.expensive_active > 0 {
            g.expensive_active -= 1;
        }
        self.expensive_finished.fetch_add(1, Ordering::Relaxed);
    }

    /// Register or observe a mutation operation id in the replay window.
    ///
    /// Idempotent retries of the same `(principal, operation_id)` within TTL
    /// return [`ReplayStatus::Retry`] and do not consume extra capacity.
    /// A brand-new id when the window is at capacity yields `resource_limit`.
    pub fn register_operation_id(
        &self,
        principal_id: &str,
        operation_id: &str,
    ) -> Result<ReplayStatus, Error> {
        let pid = bound_principal(principal_id);
        let oid = bound_op_id(operation_id);
        if oid.is_empty() {
            return Ok(ReplayStatus::Fresh);
        }
        let key = format!("{pid}\0{oid}");
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        self.evict_replay_locked(&mut g, now);
        if let Some(inserted) = g.replay_index.get(&key) {
            if now.duration_since(*inserted) < self.limits.replay_ttl {
                self.replay_retries.fetch_add(1, Ordering::Relaxed);
                return Ok(ReplayStatus::Retry);
            }
            // Expired entry still in map until eviction — treat as fresh.
        }
        if g.replay_index.len() >= self.limits.replay_capacity && !g.replay_index.contains_key(&key)
        {
            self.replay_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(Error::ResourceLimit(format!(
                "operation-id replay window full (capacity {})",
                self.limits.replay_capacity
            )));
        }
        g.replay_index.insert(key.clone(), now);
        g.replay_order.push_back((now, pid, oid));
        self.replay_fresh.fetch_add(1, Ordering::Relaxed);
        Ok(ReplayStatus::Fresh)
    }

    fn evict_replay_locked(&self, g: &mut AdmissionInner, now: Instant) {
        let ttl = self.limits.replay_ttl;
        while let Some((t, pid, oid)) = g.replay_order.front().cloned() {
            if now.duration_since(t) < ttl && g.replay_index.len() <= self.limits.replay_capacity {
                break;
            }
            g.replay_order.pop_front();
            let key = format!("{pid}\0{oid}");
            // Only remove if the index still points at this insert generation.
            if g.replay_index.get(&key).copied() == Some(t) {
                g.replay_index.remove(&key);
            }
        }
        // Capacity hard cap: drop oldest until under capacity.
        while g.replay_index.len() > self.limits.replay_capacity {
            if let Some((t, pid, oid)) = g.replay_order.pop_front() {
                let key = format!("{pid}\0{oid}");
                if g.replay_index.get(&key).copied() == Some(t) {
                    g.replay_index.remove(&key);
                }
            } else {
                break;
            }
        }
    }
}

/// RAII guard that releases one expensive-op slot on drop.
#[derive(Debug)]
pub struct ExpensiveGuard {
    controller: Option<Arc<AdmissionController>>,
}

impl Drop for ExpensiveGuard {
    fn drop(&mut self) {
        if let Some(c) = self.controller.take() {
            c.end_expensive();
        }
    }
}

fn bound_principal(id: &str) -> String {
    let s = id.replace(['\n', '\r', '\0'], " ");
    if s.len() <= 64 {
        s
    } else {
        s.chars().take(64).collect()
    }
}

fn bound_op_id(id: &str) -> String {
    let s = id.trim();
    if s.len() <= 64 {
        s.to_string()
    } else {
        s.chars().take(64).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_rate_rejects_after_budget() {
        let c = AdmissionController::new(AdmissionLimits {
            global_max_rps: 3,
            per_principal_max_rps: 100,
            ..AdmissionLimits::for_tests()
        });
        assert!(c.admit_rpc("a").is_ok());
        assert!(c.admit_rpc("a").is_ok());
        assert!(c.admit_rpc("b").is_ok());
        let err = c.admit_rpc("c").unwrap_err();
        assert!(matches!(err, Error::ResourceLimit(_)));
        assert_eq!(c.stats().rate_rejected, 1);
        assert_eq!(c.stats().admitted_rpcs, 3);
    }

    #[test]
    fn per_principal_rate_independent() {
        let c = AdmissionController::new(AdmissionLimits {
            global_max_rps: 100,
            per_principal_max_rps: 2,
            ..AdmissionLimits::for_tests()
        });
        assert!(c.admit_rpc("alice").is_ok());
        assert!(c.admit_rpc("alice").is_ok());
        assert!(c.admit_rpc("alice").is_err());
        // Other principal still allowed.
        assert!(c.admit_rpc("bob").is_ok());
    }

    #[test]
    fn auth_lockout_after_failures() {
        let c = AdmissionController::new(AdmissionLimits {
            max_auth_failures: 3,
            auth_lockout: Duration::from_secs(60),
            ..AdmissionLimits::for_tests()
        });
        let tok = Some("bad-token");
        for _ in 0..3 {
            c.check_auth_lockout(tok).unwrap();
            c.record_auth_failure(tok);
        }
        let err = c.check_auth_lockout(tok).unwrap_err();
        assert!(matches!(err, Error::AuthenticationFailed(_)));
        assert!(c.stats().auth_lockouts >= 1);
        // Wrong-token material never stored as map keys (hash only).
        let g = c.inner.lock().unwrap();
        for k in g.auth.keys() {
            let s = k.to_string();
            assert!(!s.contains("bad-token"));
        }
    }

    #[test]
    fn connect_churn_limit() {
        let c = AdmissionController::new(AdmissionLimits {
            max_connects_per_window: 2,
            connect_window: Duration::from_secs(60),
            ..AdmissionLimits::for_tests()
        });
        assert!(c.try_admit_connect());
        assert!(c.try_admit_connect());
        assert!(!c.try_admit_connect());
        assert_eq!(c.stats().connect_churn_rejected, 1);
    }

    #[test]
    fn expensive_concurrency_budget() {
        let c = AdmissionController::new(AdmissionLimits {
            max_expensive_concurrent: 1,
            ..AdmissionLimits::for_tests()
        });
        let g1 = c.try_begin_expensive("find").unwrap();
        let err = c.try_begin_expensive("scan_json").unwrap_err();
        assert!(matches!(err, Error::ResourceLimit(_)));
        drop(g1);
        let g2 = c.try_begin_expensive("find").unwrap();
        drop(g2);
        // Cheap ops never take a slot.
        let g3 = c.try_begin_expensive("ping").unwrap();
        assert!(g3.controller.is_none());
    }

    #[test]
    fn replay_window_fresh_and_retry() {
        let c = AdmissionController::new(AdmissionLimits {
            replay_capacity: 2,
            replay_ttl: Duration::from_secs(60),
            ..AdmissionLimits::for_tests()
        });
        assert_eq!(
            c.register_operation_id("p", "op-1").unwrap(),
            ReplayStatus::Fresh
        );
        assert_eq!(
            c.register_operation_id("p", "op-1").unwrap(),
            ReplayStatus::Retry
        );
        assert_eq!(
            c.register_operation_id("p", "op-2").unwrap(),
            ReplayStatus::Fresh
        );
        // Capacity full for a new id.
        let err = c.register_operation_id("p", "op-3").unwrap_err();
        assert!(matches!(err, Error::ResourceLimit(_)));
        assert_eq!(c.stats().replay_rejected, 1);
        assert_eq!(c.stats().replay_retries, 1);
    }

    #[test]
    fn profile_tag() {
        assert_eq!(ADMISSION_PROFILE, "residiuum-admission-v1");
    }
}
