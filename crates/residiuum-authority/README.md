# residiuum-authority

**AGPL-3.0-or-later** local-only heap authority ceremony tool (`HEAP_SPEC` HP-005).

Issues HeapKeys and commits restart-safe genesis through a two-slot authority
store under `authority_root`. The qualified data server (`residiuum-server`)
MUST NOT depend on this crate or any concrete `MasterKeyProvider`.

## Embedded product bootstrap

An embedded application's local composition root can provision and reopen an
exact set of named Heaps through the real authority path:

```rust,no_run
use residiuum_authority::{
    bootstrap_development_file_product, DevelopmentFileProductBootstrap,
    ProductHeapRequest, Rights,
};

let application_rights = Rights::from_bits_certificate(
    Rights::READ.bits()
        | Rights::WRITE.bits()
        | Rights::INDEX_ADMIN.bits()
        | Rights::HEAP_ADMIN.bits(),
)?;
let bootstrap = DevelopmentFileProductBootstrap::new(
    "./store",
    "./authority",
    "./secrets/gremlin-authority.v1.cbor",
    "io.koderra.gremlin",
    vec![
        ProductHeapRequest::new("tinker", application_rights),
        ProductHeapRequest::new("gremlin", application_rights),
    ],
);
let authorized = bootstrap_development_file_product(&bootstrap)?;
// Pass authorized.heaps[i].capability to SDK Client::open_named_heap.
# Ok::<(), Box<dyn std::error::Error>>(())
```

The ceremony binds one stable physical deployment identity to every Heap,
persists its interruption-recovery state before authority commit, validates
existing authority and descriptor chains on restart, renews signed HeapKeys at
the frozen 90-day limit, and returns non-serializable `HeapCap`s. Heap names,
order, rights, product identity, deployment path, and authority path are frozen
by the credential file; drift fails closed.

The concrete credential file contains master seeds. On Unix it is written
atomically with mode `0600`, rejects broader permissions and symlinks, and is
explicitly a development/local-embedded key provider. A production-qualified
composition must supply an OS keystore, TPM, HSM, or remote-signer adapter. Do
not link this crate into `residiuum-server`.

```bash
residiuum-authority --license
residiuum-authority genesis --help
```
