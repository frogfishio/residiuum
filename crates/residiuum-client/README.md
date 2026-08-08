# residiuum-client

Thin **MIT** network primitives for Residiuum: length-prefixed framed JSON RPC
(`residiuum-rpc-v1`), hello/welcome handshake, and feature negotiation.

This crate has **no** dependency on the store, cluster, or server. Use it when
you need to speak the Residiuum wire protocol from another language runtime,
a proxy, a test harness, or a custom client. Application collection APIs live
in [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk); TCP serve lives in
[`residiuum-server`](https://crates.io/crates/residiuum-server).

## When to use this crate

| You want… | Use |
|-----------|-----|
| Open a local `.residiuum` store and put/get JSON | [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk) |
| Connect over the network with the full collection API | [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk) (`Residiuum::connect`) |
| Only framing + handshake (interop, tools, other languages) | **`residiuum-client`** (this crate) |
| Accept TCP connections and dispatch RPCs | [`residiuum-server`](https://crates.io/crates/residiuum-server) |

## Install

```toml
[dependencies]
residiuum-client = "0.2.3"
```

Or: `cargo add residiuum-client`

## Protocol overview

Transport and application encoding are separate:

1. **Frame** — big-endian `u32` length prefix + UTF-8 JSON payload.
2. **Handshake** — first framed messages negotiate protocol major, max frame
   size, and feature tokens before any application RPC.
3. **Application** — JSON RPC request/response objects ride inside frames
   (defined by the server/SDK layer, not this crate).

Profile tag: `PROTOCOL_PROFILE` = `residiuum-rpc-v1`. Draft interoperability label:
`RPC_WIRE_LABEL` = `1.0-draft`.

Required features for a successful handshake:

- `json-rpc-v1` — length-prefixed JSON application RPCs
- `receipts-v1` — write/delete receipts must include required fields
- `idempotency-v1` — client-supplied `operation_id` for mutation idempotency

Legacy newline-delimited JSON is retained only as an explicit **diagnostic**
profile for local debugging (for example with `nc`). Production clients and
servers require the framed handshake.

## Quick example

```rust
use residiuum_client::{
    client_handshake, encode_frame, read_frame, write_frame, write_json_frame,
    DEFAULT_MAX_FRAME_BYTES, PROTOCOL_PROFILE, RPC_WIRE_LABEL,
};
use std::io::Cursor;

// Encode a length-prefixed frame around an application JSON body.
let body = br#"{"op":"ping"}"#;
let frame = encode_frame(body)?;
assert!(frame.len() == 4 + body.len());

// Round-trip through an in-memory stream.
let mut buf = Vec::new();
write_frame(&mut buf, body)?;
let mut cur = Cursor::new(buf);
let got = read_frame(&mut cur, DEFAULT_MAX_FRAME_BYTES)?;
assert_eq!(got.as_deref(), Some(body.as_slice()));

// Handshake helpers (client side) negotiate features against a live server.
// See `client_handshake` / `server_handshake` in the API docs.
assert_eq!(PROTOCOL_PROFILE, "residiuum-rpc-v1");
assert_eq!(RPC_WIRE_LABEL, "1.0-draft");
# Ok::<(), residiuum_client::Error>(())
```

## API surface

| Item | Role |
|------|------|
| `encode_frame` / `write_frame` / `read_frame` | Length-prefixed frame codec |
| `write_json_frame` | Serialize a value and write one frame |
| `client_handshake` / `server_handshake` | Hello / welcome / reject exchange |
| `negotiate_features` / `negotiate_max_frame` | Feature and size agreement |
| `Handshake` / `HandshakeMsg` / `NegotiatedSession` | Control-plane types |
| `PROTOCOL_MAJOR` / `PROTOCOL_MINOR` | Negotiated version constants |
| `REQUIRED_FEATURES` | Features this build always requires |
| `REQUIRED_WRITE_RECEIPT_FIELDS` / `REQUIRED_DELETE_RECEIPT_FIELDS` | Receipt field contracts |
| `Error` / `ErrorCode` | Stable machine-readable failure codes |
| `RequestId` / `OperationId` | Stable request and mutation identities |
| `TerminalOutcome` / `RetryDisposition` | Closed outcome and retry contracts shared by SDK clients |

## Related crates

| Crate | License | Role |
|-------|---------|------|
| [`residiuum-sdk`](https://crates.io/crates/residiuum-sdk) | MPL-2.0 | Collection API (embedded + remote) |
| [`residiuum-server`](https://crates.io/crates/residiuum-server) | AGPL-3.0-or-later | TCP accept loop and RPC dispatch |
| [`residiuum-format`](https://crates.io/crates/residiuum-format) | MIT | On-disk survival wire format (not network RPC) |

## License

MIT. Independent of the AGPL server and cluster crates so closed and open apps
can speak the wire protocol without taking server copyleft.

Part of [Residiuum](https://github.com/frogfishio/dingodb). Multi-tier license map:
[doc/reference/operations/LICENSING.md](https://github.com/frogfishio/dingodb/blob/main/doc/reference/operations/LICENSING.md).
