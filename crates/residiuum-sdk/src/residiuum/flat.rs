//! Legacy flat database handle (`legacy-flat-sdk` feature / CPR-001).
//!
//! This surface uses deployment-global collection names and is **not**
//! `residiuum-heap-v1` qualified. Prefer [`crate::Residiuum::open_deployment`] +
//! [`crate::Heap`] for isolation claims.

#[cfg(feature = "cluster")]
use crate::cluster_backend::ClusterBackend;
use crate::collection::Collection;
use crate::error::Error;
use crate::heap::ResidiuumDeployment;
use crate::multi_query::MultiQuery;
use crate::remote::{parse_residiuum_url, ConnectOptions, RemoteClient};
use crate::sda_query::SdaTextQuery;
use crate::subject::validate_collection_name;
#[cfg(feature = "cluster")]
use residiuum_cluster::ClusterConfig;
use residiuum_store::Store;
use std::path::{Path, PathBuf};

/// Embedded, remote, or (with `cluster` feature) clustered Residiuum handle.
///
/// **Claim:** legacy flat surface (`FLAT_COLLECTION_SURFACE_LABEL`); not Gate H6.
///
/// ```ignore
/// let mut db = Residiuum::open("./app.residiuum")?;
/// let mut users = db.collection("users")?;
/// users.put("user-42", &serde_json::json!({"name": "Alice"}))?;
/// ```
///
/// Remote (Stage 7):
/// ```ignore
/// let mut db = Residiuum::connect("residiuum://localhost:7434/app")?;
/// // or with auth / deadlines / connect retry:
/// let mut db = Residiuum::connect_with(
///     "residiuum://localhost:7434/app",
///     ConnectOptions::new().auth_token("secret"),
/// )?;
/// ```
///
/// Cluster (Stage 8d, requires feature `cluster`) — same collection API;
/// partition routes are cached client-side:
/// ```ignore
/// let mut db = Residiuum::create_cluster(
///     residiuum_cluster::ClusterConfig::dependable_local("./cluster")
/// )?;
/// // or open an existing cluster root:
/// let mut db = Residiuum::open_cluster("./cluster")?;
/// db.collection("users")?.put("user-42", &serde_json::json!({"name": "Alice"}))?;
/// ```
pub struct Residiuum {
    pub(crate) backend: Backend,
}

pub(crate) enum Backend {
    Local(Store),
    Remote(RemoteClient),
    #[cfg(feature = "cluster")]
    Cluster(ClusterBackend),
}

impl Residiuum {
    /// Open an existing store at `path`, or create one with safe defaults.
    ///
    /// **Legacy flat surface (CPR-001):** collection names are deployment-global
    /// and not bound by `HeapCap`. Prefer [`Self::open_deployment`] for heap work.
    /// Alias: [`Self::open_compatibility`].
    ///
    /// Writer opens take an exclusive store lock (DEF-020). A second writer —
    /// including `residiuum serve` while an embedded handle is open — fails until
    /// the first handle is dropped. Use [`Self::open_inspect`] for concurrent
    /// read-only doctor/parity checks.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Self::open_compatibility(path)
    }

    /// Explicit spelling of the legacy flat open (`HEAP_SPEC` §30.9).
    ///
    /// Same as [`Self::open`]. Name emphasises non-qualified compatibility use.
    pub fn open_compatibility(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self {
            backend: Backend::Local(Store::open(path)?),
        })
    }

    /// Open a store directory as a **deployment host** (heap-bound; no flat data API).
    pub fn open_deployment(path: impl AsRef<Path>) -> Result<ResidiuumDeployment, Error> {
        ResidiuumDeployment::open(path)
    }

    /// Create a new store directory as a deployment host.
    pub fn create_deployment(path: impl AsRef<Path>) -> Result<ResidiuumDeployment, Error> {
        ResidiuumDeployment::create(path)
    }

    /// Open an **existing** store for read-only inspection (no writer lock).
    ///
    /// Suitable while another process holds the exclusive writer (for example
    /// `residiuum serve`). Mutations fail because no active writer is opened.
    pub fn open_inspect(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self {
            backend: Backend::Local(Store::open_inspect(path)?),
        })
    }

    /// Connect to a remote `residiuum serve` endpoint (`residiuum://host:port[/label]`).
    ///
    /// Uses default [`ConnectOptions`] (no auth token, 5s connect / 30s request
    /// deadlines, 3 connect attempts). Prefer [`Self::connect_with`] when the
    /// server requires a token or custom deadlines.
    ///
    /// The optional path label is informational only for Stage 7 (the server
    /// process already binds a store directory). Transport is TCP framed
    /// `residiuum-rpc-v1` JSON (or diagnostic line mode when configured).
    pub fn connect(url: impl AsRef<str>) -> Result<Self, Error> {
        Self::connect_with(url, ConnectOptions::default())
    }

    /// Connect a **qualified** remote heap via HeapKey (`HEAP_SPEC` §7.1 / HP-007).
    ///
    /// Performs TLS 1.3 + `heap-key-v1` handshake and returns a [`crate::RemoteHeap`]
    /// bound to the certificate's `HeapId`. Unlike [`Self::connect`], this path
    /// never sends a shared token and cannot select the legacy listener.
    ///
    /// Active remote surface today: process ops 1–3 (ping / live / ready).
    /// Collection data ops remain reserved until §32.4 activation.
    pub fn connect_heap(
        url: impl AsRef<str>,
        options: crate::RemoteHeapOptions,
    ) -> Result<crate::RemoteHeap, Error> {
        crate::remote_heap::connect_heap(url, options)
    }

    /// Connect with explicit connection options (authn, deadlines, retry).
    ///
    /// Application put/get APIs stay the same; only the transport policy changes
    /// (DX_SPEC §4.2). Multi-seed URLs (`residiuum://h1:p1,h2:p2/app`) try seeds in
    /// order and use the first that accepts a connection; the client may then
    /// fetch a `directory` snapshot for route caching (Stage 8d).
    ///
    /// **Legacy** relative to [`Self::connect_heap`]: token/RBAC path only.
    pub fn connect_with(url: impl AsRef<str>, options: ConnectOptions) -> Result<Self, Error> {
        let url = url.as_ref();
        let parsed = parse_residiuum_url(url)?;
        if parsed.seeds.is_empty() {
            return Err(Error::ValidationMsg("empty residiuum:// URL".into()));
        }
        let mut last_err: Option<Error> = None;
        for hostport in &parsed.seeds {
            match RemoteClient::connect_with(hostport, url.to_string(), options.clone()) {
                Ok(client) => {
                    return Ok(Self {
                        backend: Backend::Remote(client),
                    });
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err
            .unwrap_or_else(|| Error::Internal("connect failed with no seed errors".into())))
    }

    /// Create a new multi-node cluster and return a SDK handle (Stage 8d).
    ///
    /// Requires the `cluster` feature (AGPL `residiuum-cluster`).
    ///
    /// Ordinary collection put/get/delete use the same API as embedded/server;
    /// the client caches partition routes and refreshes on stale placement
    /// (CLUSTER_SPEC §13).
    #[cfg(feature = "cluster")]
    pub fn create_cluster(cfg: ClusterConfig) -> Result<Self, Error> {
        Ok(Self {
            backend: Backend::Cluster(ClusterBackend::create(cfg)?),
        })
    }

    /// Open an existing cluster root directory as a SDK handle (Stage 8d).
    ///
    /// Requires the `cluster` feature (AGPL `residiuum-cluster`).
    #[cfg(feature = "cluster")]
    pub fn open_cluster(root: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self {
            backend: Backend::Cluster(ClusterBackend::open(root)?),
        })
    }

    /// Whether this handle is a remote connection (single-node or multi-seed).
    pub fn is_remote(&self) -> bool {
        matches!(self.backend, Backend::Remote(_))
    }

    /// Whether this handle is an in-process cluster.
    pub fn is_cluster(&self) -> bool {
        #[cfg(feature = "cluster")]
        {
            matches!(self.backend, Backend::Cluster(_))
        }
        #[cfg(not(feature = "cluster"))]
        {
            false
        }
    }

    /// Borrow the cluster backend (Stage 8d tests / ops).
    #[cfg(feature = "cluster")]
    pub fn cluster_backend_mut(&mut self) -> Result<&mut ClusterBackend, Error> {
        match &mut self.backend {
            Backend::Cluster(c) => Ok(c),
            _ => Err(Error::RemoteUnsupported("cluster_backend_mut")),
        }
    }

    /// Filesystem root of this store (embedded or cluster root).
    pub fn path(&self) -> Option<&Path> {
        match &self.backend {
            Backend::Local(s) => Some(s.path()),
            Backend::Remote(_) => None,
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => Some(c.root()),
        }
    }

    /// Store / cluster identifier (16 bytes).
    pub fn store_id(&self) -> [u8; 16] {
        match &self.backend {
            Backend::Local(s) => s.store_id(),
            Backend::Remote(c) => c.store_id(),
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.store_id(),
        }
    }

    /// Borrow a named collection handle (legacy flat namespace).
    ///
    /// Names are **deployment-global** — not heap-scoped. For isolation use
    /// [`crate::Heap::collection`] under a `HeapCap`.
    ///
    /// Collection access is lazy: no disk mutation occurs until the first write
    /// (embedded). Remote sends RPCs on each method call.
    pub fn collection(&mut self, name: impl Into<String>) -> Result<Collection<'_>, Error> {
        let name = name.into();
        validate_collection_name(&name)?;
        Ok(Collection::new(&mut self.backend, name))
    }

    /// Multi-collection join query axis (equijoin → optional SDA normalisation).
    ///
    /// Unlike [`Collection::query`] (single-collection filters), this builds a
    /// SQL/Mongo-ish multi-table query: scan/filter each named collection, hash
    /// equijoin on `X = Y`, then either return the rough joined bag or pass it
    /// through pure SDA for projection/normalisation.
    ///
    /// For people who prefer to **write ENR + SDA as text** (match bags, `one!`,
    /// attach/`+`, full comprehensions) instead of fluent equijoins, use
    /// [`Self::sda_query`] / [`Collection::sda`].
    ///
    /// ```ignore
    /// let bag = db
    ///     .query()
    ///     .from("orders")
    ///     .where_eq("status", "paid")
    ///     .join("customers").on("customer_id", "id")
    ///     .join("products").on("product_id", "id")
    ///     .collect()?;
    ///
    /// let shaped = db
    ///     .query()
    ///     .from("orders")
    ///     .join("customers").on("customer_id", "id")
    ///     .map_sda(r#"{ yield row | row in input }"#)?;
    /// ```
    ///
    /// Client-side only: not a distributed relational planner. Bound inputs
    /// with per-source filters / budgets when collections are large.
    pub fn query(&mut self) -> MultiQuery<'_> {
        MultiQuery::new(self)
    }

    /// Multi-collection **SDA/ENR text** query axis (DX_SPEC §7.6 companion).
    ///
    /// Bind named collections under `input` as a map of document arrays **and**
    /// as top-level free names, then run pure SDA — including the ENR1 kernel
    /// (`Match`, `enrich`, `one?` / `one!`, `merge` / `+` attach, `asBag`).
    /// Complementary to [`Self::query`]:
    ///
    /// | Axis | Surface | Join policy |
    /// |------|---------|-------------|
    /// | [`Self::query`] | Fluent `from` / `join` / `on` | Host hash equijoin |
    /// | [`Self::sda_query`] / [`Self::enr_query`] | Text program | ENR1 match + attach |
    ///
    /// Preferred surface (ENR1 `Match` + `enrich` pipe):
    ///
    /// ```ignore
    /// let out = db
    ///     .enr_query()
    ///     .bind("orders")
    ///     .bind("customers")
    ///     .run(r#"
    ///       orders
    ///       |> enrich {
    ///           customer:
    ///             one!(
    ///               Match(
    ///                 l,
    ///                 customers,
    ///                 getPath(l, Seq["customer_id"]),
    ///                 getPath(r, Seq["id"])
    ///               )
    ///             )
    ///         }
    ///       |> refine {
    ///           yield o + Map{
    ///             "customer_name" -> getPath(o, Seq["customer", "name"])
    ///           }
    ///           | o in _
    ///         }
    ///     "#)?;
    /// ```
    ///
    /// Verbose form (`bindOpt` + comprehension) still works for full SDA control.
    pub fn sda_query(&mut self) -> SdaTextQuery<'_> {
        SdaTextQuery::new(self)
    }

    /// Alias of [`Self::sda_query`] — multi-collection ENR1/SDA text path.
    ///
    /// Prefer this name when the program is enrichment-shaped (`Match` /
    /// `enrich` / `one!`) rather than pure SDA projection.
    pub fn enr_query(&mut self) -> SdaTextQuery<'_> {
        self.sda_query()
    }

    /// Convenience: bind each collection name under `input.<name>` (and as a
    /// free name) and run pure SDA/ENR1 text (no per-source filters).
    ///
    /// Equivalent to chaining [`SdaTextQuery::bind`] then [`SdaTextQuery::run`].
    pub fn sda(&mut self, collections: &[&str], program: &str) -> Result<serde_json::Value, Error> {
        let mut q = self.sda_query();
        for name in collections {
            q = q.bind(*name);
        }
        q.run(program)
    }

    /// Number of live subjects across all collections (embedded).
    ///
    /// Remote issues a `store_info` RPC. Cluster scans online partitions.
    pub fn live_count(&mut self) -> Result<usize, Error> {
        match &mut self.backend {
            Backend::Local(s) => Ok(s.live_count()),
            Backend::Remote(c) => {
                let (_path, _id, n) = c.store_info()?;
                Ok(n)
            }
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.live_count_approx(),
        }
    }

    /// Rebuild the primary index from segment files (catalog-free). Embedded only.
    pub fn rebuild_index(&mut self) -> Result<(), Error> {
        match &mut self.backend {
            Backend::Local(s) => {
                s.rebuild_index()?;
                Ok(())
            }
            Backend::Remote(_) => Err(Error::RemoteUnsupported("rebuild_index")),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported("rebuild_index")),
        }
    }

    /// Rebuild derived collection catalogs from the primary index. Embedded only.
    pub fn rebuild_catalogs(&mut self) -> Result<(), Error> {
        match &mut self.backend {
            Backend::Local(s) => {
                s.rebuild_catalogs()?;
                Ok(())
            }
            Backend::Remote(_) => Err(Error::RemoteUnsupported("rebuild_catalogs")),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported("rebuild_catalogs")),
        }
    }

    /// Collection names known from the derived catalog (sorted).
    pub fn list_collections(&mut self) -> Result<Vec<String>, Error> {
        match &mut self.backend {
            Backend::Local(s) => Ok(s.list_collections()),
            Backend::Remote(c) => c.list_collections(),
            #[cfg(feature = "cluster")]
            Backend::Cluster(c) => c.list_collections(),
        }
    }

    /// Compact live state into a new sealed segment (sources retained). Embedded only.
    pub fn compact_live(&mut self) -> Result<residiuum_store::CompactReport, Error> {
        match &mut self.backend {
            Backend::Local(s) => Ok(s.compact_live()?),
            Backend::Remote(_) => Err(Error::RemoteUnsupported("compact_live")),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported("compact_live")),
        }
    }

    /// Write a derived checkpoint with declared coverage. Embedded only.
    pub fn checkpoint(
        &self,
        coverage: &str,
    ) -> Result<(residiuum_store::CheckpointMeta, PathBuf), Error> {
        match &self.backend {
            Backend::Local(s) => Ok(s.checkpoint(coverage)?),
            Backend::Remote(_) => Err(Error::RemoteUnsupported("checkpoint")),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported("checkpoint")),
        }
    }

    /// Access the underlying raw store (embedded only).
    ///
    /// **Bypasses heap façades** — not part of the Gate H6 claim surface (CPR-001).
    pub fn store(&self) -> Result<&Store, Error> {
        match &self.backend {
            Backend::Local(s) => Ok(s),
            Backend::Remote(_) => Err(Error::RemoteUnsupported("store()")),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported("store()")),
        }
    }

    /// Mutable access to the underlying raw store (embedded only).
    ///
    /// **Bypasses heap façades** — not part of the Gate H6 claim surface (CPR-001).
    pub fn store_mut(&mut self) -> Result<&mut Store, Error> {
        match &mut self.backend {
            Backend::Local(s) => Ok(s),
            Backend::Remote(_) => Err(Error::RemoteUnsupported("store_mut()")),
            #[cfg(feature = "cluster")]
            Backend::Cluster(_) => Err(Error::RemoteUnsupported("store_mut()")),
        }
    }

    /// Path buffer for callers that need an owned root (embedded or cluster).
    pub fn path_buf(&self) -> Option<PathBuf> {
        self.path().map(|p| p.to_path_buf())
    }
}
