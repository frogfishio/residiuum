# Residiuum async driver v1 registries

Status: **embedded DRV-1/DRV-5 implementation candidate; remote/query pending**.

Authority: `doc/todo/application-driver/ASYNC_DRIVER_SPINE_SPEC.md`.

This directory records the contract, current architecture, and implementation
status. The SDK now contains one deployment-level embedded connection with
cloneable capability-bound Heap handles, bounded scheduling, and idempotent
mutations. It does not yet claim the remote pool, streamed RQL, or
cancellation-after-dispatch portions of the full driver.

- `contract-v1.json` freezes request states, terminal outcomes, retry
  dispositions, required wire features, and v1 resource defaults.
- `current-runtime-inventory-v1.json` records the legacy synchronization and
  blocking paths that the new driver must replace rather than conceal.
- `implementation-status-v1.json` is the closed claim/evidence/residual matrix.

Run `bash scripts/verify-driver-drv0.sh` for registry, architecture, scheduler,
and embedded integration checks.
