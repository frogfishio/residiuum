# Application Driver Spine

Status: **developer-ready; execution governed by `CRITICAL_PATH.md`**

Normative specification:
[ASYNC_DRIVER_SPINE_SPEC.md](./ASYNC_DRIVER_SPINE_SPEC.md).

Current embedded application handoff:
[GREMLIN_EMBEDDED_HANDOFF.md](./GREMLIN_EMBEDDED_HANDOFF.md).

Driver work remains governed here. Atomics implementation is now separately
admitted by `CRITICAL_PATH.md` §1.1 and must follow
[the Atomics programme](../atomics/README.md); it reuses this async client and
does not broaden driver scope to non-Rust bindings or cluster work.
