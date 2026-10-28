<div align="center">

# MMA Network Protocol

**A binary TCP protocol with a configurable wire format — Rust core, C ABI.**

**English** · [Русский](README.ru.md)

</div>

> **Status:** pre-release `0.1.0` · wire protocol version `1` · the API and the C ABI may change without notice.

MMA is a private, binary, request/response and channel protocol over TCP. Its defining feature: **there is no single fixed wire format.** The byte layout of a frame is defined by a configuration (`FramerConfig`) that both peers share, so two deployments with different configurations look like two different, incompatible protocols on the wire.

This document specifies the frame format, the connection model, the Rust API and the C ABI.

## Contents

- [1. Overview](#1-overview)
- [2. Quick start](#2-quick-start)
- [3. Frame format](#3-frame-format)
- [4. Options](#4-options)
- [5. Routes and connection model](#5-routes-and-connection-model)
- [6. Errors](#6-errors)
- [7. Rust API](#7-rust-api)
- [8. C ABI](#8-c-abi)
- [9. Compatibility and versioning](#9-compatibility-and-versioning)
- [10. Security](#10-security)
- [11. Status and known limitations](#11-status-and-known-limitations)
- [12. Repository layout](#12-repository-layout)

---

## 1. Overview

### 1.1. The idea: no standard wire format

Most protocols (HTTP, RESP, MQTT, gRPC) fix their byte layout in a specification, so any sniffer can recognise them by signature. MMA specifies the **rules**, not the layout. The layout is chosen per deployment:

| `FramerConfig` field | What it defines |
|---|---|
| `header_length` | header size, 17–255 bytes |
| `opcode_pos`, `version_pos`, `route_len_pos`, `options_count_pos` | which header byte holds each one-byte field |
| `big_bytes_config` | which of the three 4-byte slots at the end of the header holds `options_len`, `payload_len`, `req_id` |
| `route_order`, `options_order`, `payload_order` | order of the body sections (a permutation of 1..3) |
| `options_key_first` | key/value order inside an option |
| spare header bytes | zero on encode, ignored on decode |

Consequences:

- A frame has no constant signature (no magic number); field positions and section order differ between deployments. This makes naive pattern matching harder, but it is **obfuscation, not a security mechanism**.
- The configuration is a **shared wire profile** that client and server must agree on out of band. Both sides must use identical values.
- The **logical model** is constant: `opcode`, `version`, `req_id`, `route`, `options`, `payload`.
- MMA provides **no encryption and no authentication**. Run it over TLS or inside a trusted network (see [Security](#10-security)).
- There is no handshake yet: if the configurations differ, the server misparses frames — typically answering `BadRequest` and closing the connection.

### 1.2. Purpose

MMA is meant for connecting your own clients and servers when you want:

- compact framing with explicit lengths;
- a wire format you can tune per deployment;
- two interaction modes — **Once** (request → one response) and **Channel** (persistent connection with server push);
- one server engine usable from Rust and C.

Out of scope: encryption, authentication, service discovery, compatibility with third-party clients, delivery guarantees across restarts. The crate does **not** ship a client; build one from this specification.

### 1.3. Properties

| Property | Value |
|---|---|
| Transport | TCP |
| Interaction | Request/Response (`Once`) and `Channel` |
| Encoding | Binary, layout set by `FramerConfig` |
| Wire version | `MMA_VERSION = 1` |
| Header size | min 17 bytes, default 29 |
| Max frame size | `header_length + max_message_length` (default `29 + 10 000 000`) |
| Integer byte order | big-endian |
| Interfaces | Rust (server API), C (`include/mma.h`) |

---

## 2. Quick start

### Rust server

```toml
[dependencies]
MMA = { git = "https://github.com/Edestuss0/MMA-Network-protocol" }
tokio = { version = "1", features = ["full"] }
bytes = "1"
```

```rust
use MMA::core::{commands::{Request, Response}, config::Config,
                router::Router, server::Server};
use bytes::Bytes;

async fn upper(req: Request, mut res: Response) -> Response {
    res.payload = Bytes::from(req.payload.to_ascii_uppercase());
    res
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut server = Server::new(Config::default()).await?;
    let mut router = Router::new();
    router.register_route("UP".to_string(), upper).expect("route");
    server.run(router).await
}
```

Run the bundled example: `cargo run --example main_server`.

### Minimal client (Python, default layout)

```python
import socket, struct

HEADER_LEN = 29

def recv_exact(sock, size):
    buf = bytearray()
    while len(buf) < size:
        part = sock.recv(size - len(buf))
        if not part:
            raise EOFError("connection closed")
        buf.extend(part)
    return bytes(buf)

def make_frame(opcode, route, payload=b"", req_id=1):
    h = bytearray(HEADER_LEN)
    h[0], h[1], h[2], h[3] = opcode, 1, len(route), 0   # opcode, version, route_len, options_count
    h[17:29] = struct.pack(">III", 0, len(payload), req_id)  # options_len, payload_len, req_id
    return bytes(h) + route + payload

with socket.create_connection(("127.0.0.1", 8080)) as s:
    s.sendall(make_frame(2, b"UP", b"hello", req_id=1))
    h = recv_exact(s, HEADER_LEN)
    payload_len = struct.unpack(">I", h[21:25])[0]
    print(h[0], recv_exact(s, payload_len))   # 1 b'HELLO'
```

### C server

```bash
cargo build --release
gcc -std=c11 -Wall -Wextra -pthread -Iinclude examples/main_server.c \
    -Ltarget/release -lMMA -Wl,-rpath,"$(pwd)/target/release" -o mma_server_c
```

See [8. C ABI](#8-c-abi).

---

## 3. Frame format

### 3.1. Layout

```text
┌──────────────────────────────────────────────────────────────┐
│ Header — header_length bytes (≥ 17, default 29)              │
│   opcode / version / route_len / options_count               │
│       (one byte each, at configured positions)               │
│   spare bytes (zero)                                         │
│   last 12 bytes: three 4-byte big-endian slots holding       │
│       options_len, payload_len, req_id                       │
│       (slot order set by big_bytes_config)                   │
├──────────────────────────────────────────────────────────────┤
│ Body — three sections in the order set by                    │
│        route_order / options_order / payload_order           │
│   Route    route_len bytes (UTF-8)                           │
│   Options  options_len bytes                                 │
│   Payload  payload_len bytes                                 │
└──────────────────────────────────────────────────────────────┘
```

Responses carry no route section (`route_len = 0`).

### 3.2. Header fields

Offsets are 0-based from the start of the frame; `H = header_length`. Configured positions (`*_pos`) are 1-based, so offset = `pos − 1`. The offsets of the three 4-byte fields below are for the default slot order.

| Field | Offset | Size | Type | Meaning | Validation |
|---|---:|---:|---|---|---|
| `opcode` | `opcode_pos − 1` (def. 0) | 1 | `u8` | Request or response type ([3.4](#34-opcodes)) | request: 1, 2 or 3 |
| `version` | `version_pos − 1` (def. 1) | 1 | `u8` | Protocol version, currently `1` | checked by the router |
| `route_len` | `route_len_pos − 1` (def. 2) | 1 | `u8` | Route length; `0` in responses | 0–255, route must be UTF-8 |
| `options_count` | `options_count_pos − 1` (def. 3) | 1 | `u8` | Number of options | 0–255, must match parsed count |
| spare | rest of `[0, H−12)` | `H − 16` | — | Reserved for layout variation | zero on encode, ignored on decode |
| `options_len` | `H − 12` (def.) | 4 | `u32` BE | Size of the options section | counts toward `max_message_length` |
| `payload_len` | `H − 8` (def.) | 4 | `u32` BE | Size of the payload | counts toward `max_message_length` |
| `req_id` | `H − 4` (def.) | 4 | `u32` BE | Request identifier, echoed in the response | any `u32` |

What each field is for:

- **`opcode`** — what the frame is and how it is handled.
- **`version`** — protocol version; lets semantics evolve without changing the layout.
- **`route_len`** — boundary of the route section; the route selects the server handler.
- **`options_count`** — number of key/value pairs; together with `options_len` it validates the section.
- **`options_len`, `payload_len`** — section sizes; they determine the frame length.
- **`req_id`** — correlates responses with requests. Responses may arrive out of order, so use unique `req_id`s among in-flight requests.
- **spare bytes** — free header space, used to vary the layout; carry no meaning.

### 3.3. Frame length

```text
body_len  = route_len + options_len + payload_len    (checked addition)
frame_len = header_length + body_len                 (checked addition)
```

`body_len` is checked against `max_message_length` before any allocation. A frame can arrive in several TCP reads, and several frames can arrive in one; boundaries come only from the length fields.

### 3.4. Opcodes

**Request opcodes (client → server)**

| Name | Code | Valid in state | Purpose |
|---|---:|---|---|
| `ChannelConnect` | 1 | `Fresh` | Switch the connection to Channel mode |
| `Once` | 2 | `Fresh`, `Once` | Request handled by a route handler; one response |
| `ChannelMessage` | 3 | `Channel` | Message for a channel handler; no response on success |

**Response opcodes (server → client)**

| Name | Code | Meaning | Sent when |
|---|---:|---|---|
| `Ok` | 1 | Success | Default status of a response |
| `BadRequest` | 2 | Invalid request | Decode error; wrong version; channel message without a channel |
| `Unauthorized` | 3 | Not authorised | Set by your handler |
| `Forbidden` | 4 | Forbidden | Set by your handler |
| `NotFound` | 5 | Unknown route | Payload `"No such method"` |
| `Conflict` | 6 | State conflict | Second `ChannelConnect` |
| `InternalError` | 7 | Server error | Handler panic |
| `ChannelMessage` | 8 | Server push | Delivered via `send_to_channel` |
| `Shutdown` | 9 | Server stopping | Reserved; not currently sent |
| `ChannelConnected` | 10 | Channel established | Reply to `ChannelConnect` |
| `Timeout` | 11 | Handler timed out | `request_timeout` expired |

Codes `0` and `≥ 12` are undefined. An unknown request opcode is a decode error.

### 3.5. Example frames

Default layout (`header_length = 29`, positions 1/2/3/4, order route → options → payload).

Request: `Once`, route `GET`, payload `hello`, `req_id = 1` — 37 bytes:

```text
02 01 03 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 05 00 00 00 01 47 45 54 68 65 6c 6c 6f
```

| Offset | Hex | Field | Value |
|---:|---|---|---|
| 0 | `02` | `opcode` | `Once` |
| 1 | `01` | `version` | 1 |
| 2 | `03` | `route_len` | 3 |
| 3 | `00` | `options_count` | 0 |
| 4–16 | `00 …` | spare | 13 zero bytes |
| 17–20 | `00 00 00 00` | `options_len` | 0 |
| 21–24 | `00 00 00 05` | `payload_len` | 5 |
| 25–28 | `00 00 00 01` | `req_id` | 1 |
| 29–31 | `47 45 54` | route | `"GET"` |
| 32–36 | `68 65 6c 6c 6f` | payload | `"hello"` |

Response `Ok` — 34 bytes:

```text
01 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 05 00 00 00 01 68 65 6c 6c 6f
```

The same logical request under another configuration (for example `version_pos=4`, `route_len_pos=6`, `options_count_pos=7`, order payload → options → route, as in `examples/main_server.rs`) produces entirely different bytes. That is the point of the design.

### 3.6. Layout configuration

| Field | Allowed values | Default | Effect on the wire |
|---|---|---|---|
| `header_length` | 17–255 | 29 | header size, position of the 12-byte tail |
| `opcode_pos`, `version_pos`, `route_len_pos`, `options_count_pos` | 1…`header_length − 12`, pairwise distinct | 1, 2, 3, 4 | offsets of the one-byte fields |
| `route_order`, `options_order`, `payload_order` | permutation of 1..3 | 1, 2, 3 | order of body sections |
| `big_bytes_config.options_len_order` / `payload_len_order` / `req_id_order` | permutation of 1..3 | 1, 2, 3 | slot of each 4-byte field in the tail |
| `options_key_first` | `true` / `false` | `true` | `key,value` or `value,key` inside an option |
| `max_message_length` | `u32` | 10 000 000 | not a layout parameter; a server-side size limit |

All fields except `max_message_length` are wire parameters and **must match on both sides**.

### 3.7. Malformed and incomplete frames

| Situation | Server reaction | Connection |
|---|---|---|
| Incomplete header or body | waits for more data | stays open |
| EOF in the middle of a frame | no response | closed |
| Length above `max_message_length` | `BadRequest` | closed |
| Length overflow | `BadRequest` | closed |
| Unknown request opcode | `BadRequest` | closed |
| Route is not valid UTF-8 | `BadRequest` | closed |
| Malformed options (empty key/value, duplicate key, trailing bytes, wrong count) | `BadRequest` | closed |
| Wrong `version` | `BadRequest` (`"Incorrect protocol version"`) | stays open |

After a decode error the stream cannot be resynchronised, so the connection is closed. `BadRequest` carries the `req_id` from the header when it could be read, otherwise `0`.

---

## 4. Options

Options are named parameters attached to a frame (metadata, flags, trace IDs). The protocol reserves no keys; their meaning belongs to your application.

| Property | Behaviour |
|---|---|
| Key | raw bytes, case-sensitive, 1–65535 bytes |
| Value | raw bytes, 1–65535 bytes |
| Empty key or value | rejected |
| Duplicate keys | rejected |
| Count | 0–255 per frame |
| Unknown keys | ignored by the library; available in `Request.options` |

Wire format of one option (`options_count` of them are concatenated into the options section):

```text
options_key_first = true :  [key_len u16 BE][value_len u16 BE][key][value]
options_key_first = false:  [key_len u16 BE][value_len u16 BE][value][key]
```

Example — `POW`, payload `7`, option `pow = 3`, `req_id = 2`, default layout (41 bytes):

```text
02 01 03 01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 08 00 00 00 01 00 00 00 02 50 4f 57 00 03 00 01 70 6f 77 33 37
```

| Section | Bytes | Meaning |
|---|---|---|
| route (3) | `50 4f 57` | `"POW"` |
| options (8) | `00 03` `00 01` `70 6f 77` `33` | `key_len=3`, `value_len=1`, key `pow`, value `3` |
| payload (1) | `37` | `"7"` |

Header: `options_count = 1`, `options_len = 8`, `payload_len = 1`, `req_id = 2`.

Rust access: `Request::get_option(&self, key: &[u8]) -> Option<Bytes>` (no data copy). Values are raw bytes; parse them yourself and **never `unwrap` client-supplied values**. Typed accessors are not implemented.

---

## 5. Routes and connection model

### 5.1. Routes

- A route is a UTF-8 string of at most 255 bytes, case-sensitive.
- Unknown route → `NotFound` (`"No such method"`); the connection stays open.
- Registering the same route twice (also across request and channel handlers) is an error.

### 5.2. Connection states

```text
            ChannelConnect                     ChannelConnect
 Fresh ──────────────────────► Channel ◄──────────────────────┐ (Conflict, stays)
   │                             │  ▲                          │
   │ Once                        │  └── ChannelMessage ───────┘ (no response)
   ▼                             │
 Once ◄── Once (loop)            └── Once → mismatch (stays)
```

| State | Request | Result | New state |
|---|---|---|---|
| `Fresh` | `Once` | route handler | `Once` |
| `Fresh` | `ChannelConnect` | `ChannelConnected` | `Channel` |
| `Fresh` | `ChannelMessage` | `BadRequest` ("Channel not connected") | `Fresh` |
| `Once` | `Once` | route handler | `Once` |
| `Once` | `ChannelConnect` / `ChannelMessage` | `BadRequest` (type mismatch) | `Once` |
| `Channel` | `ChannelConnect` | `Conflict` | `Channel` |
| `Channel` | `ChannelMessage` | channel handler | `Channel` |
| `Channel` | `Once` | `BadRequest` (type mismatch) | `Channel` |

Use one mode per connection and do not send `Once` and `ChannelConnect` concurrently on a fresh connection.

### 5.3. Request/response (Once)

- The client chooses `req_id`; the server returns it unchanged.
- A connection can carry many requests in flight (limits: 256 per connection, `max_in_flight` globally).
- **Responses may arrive in a different order** than requests. Correlate by `req_id`.
- If a handler exceeds `request_timeout`, the server answers `Timeout` (11). Cancellation takes effect at the next `.await`; blocking code is not interrupted.
- A handler panic is caught and answered with `InternalError`; the connection stays open.
- Retrying after a timeout or disconnect is up to your application; the protocol gives no idempotency guarantee.

### 5.4. Channel mode

- `ChannelConnect` switches the connection to Channel mode; the server assigns a connection id (`conn_id`) and does not send it to the client.
- The server pushes messages with `ServerHandle::send_to_channel(conn_id, Response)`. **`conn_id = 0` broadcasts to all channel connections.** Your channel handler receives the `conn_id` of the sender connection.
- Set the pushed response's `opcode` to `ChannelMessage` (8) yourself.
- `Ok(())` from `send_to_channel` means "accepted into the broadcast queue", not "delivered". `Err(())` means there are no subscribers.
- The broadcast queue holds 1024 messages. A slow receiver loses messages (`Lagged`).
- Push messages and regular responses share one outgoing queue; their relative order is not guaranteed.

---

## 6. Errors

| Condition | Response | `req_id` | Connection | Retry? |
|---|---|---|---|---|
| Frame too large / malformed / unknown opcode | `BadRequest` | from header, else 0 | closed | no — fix the frame |
| Wrong version | `BadRequest` | request | open | no |
| Unknown route | `NotFound` | request | open | no |
| Wrong mode for the opcode | `BadRequest` | request | open | no |
| Second `ChannelConnect` | `Conflict` | request | open | no |
| Handler panic | `InternalError` | request | open | depends on the operation |
| Handler timeout | `Timeout` | request | open | if idempotent |
| TCP error / EOF | none | — | closed | on a new connection |

`Unauthorized` and `Forbidden` are available to your handlers; the library never sends them itself.

---

## 7. Rust API

Public modules: `MMA::core::{commands, config, router, server}` and `MMA::c_api`. The framer and network layers are private. The crate builds as `rlib`, `cdylib` and `staticlib`.

### 7.1. Types

| Type | Role |
|---|---|
| `Config` | server settings (`Clone`, `Debug`); `Config::default()`, `validate()` |
| `FramerConfig` | wire layout; `FramerConfig::new()` / `Default`, `validate()` |
| `Request` | `headers`, `payload: Bytes`, `route: String`, `options: Vec<(Bytes, Bytes)>` |
| `Response` | `headers`, `payload: Bytes`, `options`; created with `Response::new(req_id)` |
| `Router` | route table |
| `Server` | owns the listener; `run`, `run_until` |
| `ServerHandle` | cheap `Clone`, `Send + Sync`; sends channel messages |

### 7.2. Configuration

```rust
use MMA::core::config::{Config, FramerConfig};
use std::{net::SocketAddr, time::Duration};

let config = Config {
    address: SocketAddr::from(([0, 0, 0, 0], 8080)),
    frame_config: FramerConfig::new(),
    max_batch: 255,
    max_in_flight: 1024,
    max_connections: 128_000,
    drain_timeout: Duration::from_secs(2),
    shutdown_grace: Duration::from_secs(10),
    request_timeout: Duration::from_secs(10),
};
config.validate().expect("invalid config");
```

| Parameter | Default | Meaning |
|---|---|---|
| `max_connections` | 128 000 | simultaneous connections (≥ 1) |
| `max_in_flight` | 1024 | concurrently running handlers across the server (≥ 1) |
| `max_batch` | 255 (Rust), 16 (C default) | responses written before one flush (≥ 1) |
| `max_message_length` (in `FramerConfig`) | 10 000 000 | maximum body size in bytes |
| `request_timeout` | 10 s | handler time limit |
| `drain_timeout` | 2 s | time to flush pending responses when a connection closes |
| `shutdown_grace` | 10 s | time to wait for running tasks on shutdown before forced cancellation |

`Config::validate` rejects zero limits, an invalid `FramerConfig` and **port 0**.

### 7.3. Handlers

```rust
async fn echo(req: Request, mut res: Response) -> Response {
    res.payload = req.payload;       // Bytes, no copy
    res
}

let mut router = Router::new();
router.register_route("GET", echo)?;
```

A handler takes `Request` and `Response` by value and returns the `Response`. The status defaults to `Ok`; change `res.headers.opcode` or use `Response::bad_request(req_id, Some(payload))`, `Response::conflict(..)`, etc.

Channel handler:

```rust
use MMA::core::commands::ResponseOpcode;

router.register_channel_handler("SEND".to_string(), {
    let handle = handle.clone();                      // ServerHandle
    move |req: Request, _conn_id: u32| {
        let handle = handle.clone();
        async move {
            let mut msg = Response::new(0);
            msg.headers.opcode = ResponseOpcode::ChannelMessage;
            msg.payload = req.payload;
            let _ = handle.send_to_channel(0, msg);   // 0 = everyone
        }
    }
})?;
```

Shared state — capture an `Arc` in the closure:

```rust
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

let counter = Arc::new(AtomicU64::new(0));
router.register_route("COUNT", {
    let counter = counter.clone();
    move |_req, mut res: Response| {
        let counter = counter.clone();
        async move {
            res.payload = Bytes::from((counter.fetch_add(1, Ordering::Relaxed) + 1).to_string());
            res
        }
    }
})?;
```

Do not block inside handlers: `request_timeout` cannot cancel blocking code, and it occupies a runtime worker. Use `tokio::task::spawn_blocking` for blocking or CPU-heavy work.

### 7.4. Server lifecycle

| Method | Behaviour |
|---|---|
| `Server::new(config).await` | validates the config and binds the listener |
| `Server::from_listener(config, listener)` | uses an existing listener (the config is still validated, so port 0 is rejected) |
| `server.handle()` | returns a `ServerHandle` |
| `server.run(router).await` | serves until Ctrl+C, then shuts down gracefully |
| `server.run_until(router, fut).await` | serves until `fut` completes |
| `server.shutdown_token()` / `shutdown_server()` | cancellation token / trigger shutdown |
| `server.local_addr()` | bound address |

Shutdown: accepting stops, readers stop, running handlers get `shutdown_grace`, then everything is cancelled.

### 7.5. Errors

| Call | Error | Cause |
|---|---|---|
| `Server::new` | `io::Error` | invalid config or bind failure |
| `Router::register_route` / `register_channel_handler` | `Err` | empty or too long route, duplicate route |
| `ServerHandle::send_to_channel` | `Err(())` | no subscribers |
| `Config::validate` | `ConfigError` | zero limit, port 0, invalid `FramerConfig` |
| `Server::run` / `run_until` | `io::Error` | listener/accept failure |

---

## 8. C ABI

Header: [`include/mma.h`](include/mma.h). Library: `libMMA.so` / `libMMA.dylib` / `MMA.dll` and `libMMA.a` in `target/release/` after `cargo build --release`. Verified on Linux x86-64 only.

### 8.1. Functions

| Function | Purpose | Notes |
|---|---|---|
| `mma_server_config_default` | fill `MMA_ServerConfig` with defaults | `bind_ip` points to a static `"0.0.0.0"` |
| `mma_server_create` | validate the config, create a server (no bind yet) | free with `mma_server_destroy` |
| `mma_server_register_route` | register a request/response handler | before `start`; the route is copied |
| `mma_server_register_channel_handler` | register a channel handler | before `start` |
| `mma_server_route` | alias of `mma_server_register_route` | |
| `mma_server_start` | start an OS thread and a Tokio runtime, bind, wait for readiness | `MMA_OK` only after a successful bind |
| `mma_server_local_port` | return the port | call after `start` |
| `mma_server_get_handle` | get a server handle (clone of `ServerHandle`) | after `start`; free with `mma_server_handle_destroy` |
| `mma_server_handle_send_to_channel` | push a response to channels | `conn_id == 0` broadcasts |
| `mma_server_handle_change_config` | send a new configuration to the running server | not all fields apply at runtime |
| `mma_server_handle_destroy` | free the handle | does not stop the server |
| `mma_server_stop` | cancel and join the server thread | never call from a callback of the same server |
| `mma_server_destroy` | free the server (calls `stop`) | `NULL` is allowed |
| `mma_response_new` / `mma_response_destroy` | create / free a standalone response | for messages created outside a callback |
| `mma_response_set_status` | set the response opcode | returns the response; `inner == NULL` on error |
| `mma_response_set_payload` | set the payload (copied) | |
| `mma_response_add_option` | add an option (copied) | non-empty key/value, ≤ 65535 bytes, ≤ 255 options |

### 8.2. Error codes

| Code | Name | Meaning |
|---:|---|---|
| 0 | `MMA_OK` | success |
| −1 | `MMA_INVALID_ARGUMENT` | `NULL` or invalid argument (e.g. route not UTF-8) |
| −2 | `MMA_INVALID_CONFIG` | config rejected or `bind_ip` invalid |
| −3 | `MMA_SERVER_ERROR` | thread/runtime/bind failure |
| −4 | `MMA_SERVER_ALREADY_STARTED` | already started |

Errors are reported only through return codes; there is no last-error API. A failure inside a route callback reaches the client as `InternalError` and is not visible to the C caller.

### 8.3. Callbacks

```c
typedef MMA_Response (*MMA_RouteCallback)(
    MMA_Request request, MMA_Response response, const void *user_data);

typedef int32_t (*MMA_ChannelHandlerCallback)(
    const MMA_Request *request, uint32_t connection_id, const void *user_data);
```

A route callback is **functional**: it receives an empty `MMA_Response`, applies `mma_response_*` calls (each returns the updated response) and returns it.

Rules:

- `request` data (`payload`, `route`, `options`) is **borrowed and valid only during the callback**. Copy what you need.
- Callbacks run **concurrently on Tokio worker threads**. Synchronise shared `user_data`, and keep it alive until `mma_server_stop` returns.
- **Do not block** in a callback: it occupies a runtime worker and `request_timeout` cannot interrupt it.
- Do not call `mma_server_*` from a callback; do not throw C++ exceptions or `longjmp` out of one.
- Return a response with `inner == NULL` (or fail a setter) to signal an error; the client gets `InternalError`.
- The channel callback's return value is currently ignored.
- Ownership: inside a route callback the response belongs to the library — return it, never destroy it. A response made with `mma_response_new` needs exactly one `mma_response_destroy`.

### 8.4. Strings and buffers

| Parameter | Length | NUL | Encoding | Lifetime |
|---|---|---|---|---|
| `bind_ip` | NUL-terminated | yes | IPv4/IPv6 text | read during the call only |
| `route` | `route_len` | no | UTF-8, ≤ 255 | copied |
| `request.payload`, `request.route`, `request.options[i]` | explicit length | no | raw bytes (route UTF-8) | callback only |
| data passed to `mma_response_*` | explicit length | no | raw bytes | copied; free it after the call |

### 8.5. Type mapping

| Rust | C |
|---|---|
| `u8` / `u16` / `u32` / `u64` | `uint8_t` / `uint16_t` / `uint32_t` / `uint64_t` |
| `bool` | `uint8_t` (0/1) |
| `Duration` | `uint64_t` milliseconds (`*_ms`) |
| `SocketAddr` | `const char *bind_ip` + `uint16_t port` (port 0 rejected) |
| `Bytes` | `const uint8_t *` + `size_t` (borrowed) |
| `String` (route) | `const uint8_t *` + `size_t`, UTF-8, no NUL |
| `Vec<(Bytes, Bytes)>` | `const MMA_Option *` + `size_t` |
| `ResponseOpcode` | `uint8_t`, constants `MMA_RESPONSE_*` |
| `Server` / `ServerHandle` / `Response` | `MMA_Server *` / `MMA_ServerHandle *` / `MMA_Response` |

### 8.6. Lifetime rules

- `create` → register routes → `start` → … → `stop` → `destroy`.
- Do not call `start`, `stop` or `destroy` concurrently on the same pointer, and do not use a pointer after `destroy`. The library cannot detect double-free or use-after-free.
- `mma_server_stop` makes the server thread exit and joins it; it does not guarantee that a blocking callback has returned.
- `mma_server_handle_change_config`: new `FramerConfig` and `request_timeout` are picked up by connections; `max_batch`, `max_connections`, `max_in_flight`, the listen address and `shutdown_grace` are **not** updated. Changing wire parameters without switching clients at the same time breaks parsing.

### 8.7. Example

The full example is [`examples/main_server.c`](examples/main_server.c) (routes `GET`, `POW`, channel handler `SEND`, signal handling, server handle).

```c
#include "mma.h"

static MMA_Response echo(MMA_Request req, MMA_Response res, const void *ud) {
    (void)ud;
    res = mma_response_set_status(res, MMA_RESPONSE_OK);
    if (res.inner == NULL) return res;
    return mma_response_set_payload(res, req.payload, req.payload_len);
}
```

The wire layout is set through `config.framer` (`MMA_FramerConfig`) and must match the client.

---

## 9. Compatibility and versioning

Three versions are independent:

| What | Now | Rule |
|---|---|---|
| Wire protocol | `MMA_VERSION = 1` | bumped on incompatible frame or semantics changes |
| Rust crate | `0.1.0` | before 1.0, breaking changes may occur in any release |
| C ABI / `mma.h` | unversioned | signatures may change; no runtime version query |

Two peers are wire-compatible only if `MMA_VERSION` **and** the whole `FramerConfig` match. Adding opcodes or a mandatory field requires a new `MMA_VERSION`; an old server closes the connection on an unknown request opcode.

---

## 10. Security

- **No TLS, no authentication, no replay protection.** The layout configuration is obfuscation and is not a secret unless distributed as one. Terminate TLS in front of MMA (stunnel, a sidecar proxy) or use a trusted network.
- Treat every field from the client as untrusted. Only `route` is UTF-8-validated; payload and options are raw bytes.

| Limit | Value |
|---|---|
| Frame body | `max_message_length` (default 10 000 000), checked before allocation |
| Route | ≤ 255 bytes |
| Options | ≤ 255 per frame; key and value 1–65535 bytes |
| Connections | `max_connections` |
| In-flight requests | `max_in_flight` globally, 256 per connection |
| Handler time | `request_timeout` (async code only) |

Not covered yet: idle/read/write timeouts (slow-client attacks), a global byte budget for in-flight data, and protection against clients that do not read responses (see below).

---

## 11. Status and known limitations

MMA is pre-release. Known issues in the current version:

| Limitation | Effect | Workaround |
|---|---|---|
| Connection state is stored in a shared atomic; the `Once` state is not reliably recognised and concurrent mode changes can race | `Once` → `ChannelConnect` upgrade may succeed; two modes can be entered at once | use one mode per connection |
| No idle, read or write timeouts | slow or silent clients hold connections | enforce timeouts at a proxy/firewall |
| A client that does not read responses can stall handlers and exhaust `max_in_flight` | denial of service | limit clients at a proxy |
| An unencodable response (e.g. above `max_message_length`) stops writes on that connection | client receives no further responses | keep responses within the limit |
| Blocking code, including blocking C callbacks, is not cancelled by `request_timeout` | worker threads blocked | keep callbacks short; offload blocking work |
| Broadcast to all connections is filtered per connection | O(connections) per push | — |
| No configuration handshake | mismatched configs fail unpredictably | verify configs out of band |
| Closing the connection after a malformed frame is intended, but not covered by automated tests | — | — |
| `Shutdown` (9) is defined but never sent | clients get no shutdown notice | — |
| Port 0 is rejected by `Config::validate`, including for C and `from_listener` | no ephemeral port | choose a fixed port |
| No Rust client crate; codec tests are disabled; no benchmarks or fuzzing in the repo | — | write a client from [§3](#3-frame-format) |
| C: `MMA_Response` ownership on setter failure is fragile; duplicate registrations from C can be silently dropped at start | — | register each route once; check return codes |
| C: no ABI version query; `MMA_ChannelHandlerCallback` user-data pointer constness differs between Rust and the header | — | — |

No throughput or latency figures are published: there is no reproducible benchmark in the repository yet. `max_connections = 128 000` is a limit, not a measured capacity.

---

## 12. Repository layout

| Path | Contents |
|---|---|
| `src/protocol/framer.rs` | frame encoder/decoder (private) |
| `src/network/network.rs` | per-connection loop, state machine (private) |
| `src/core/` | `commands` (Request/Response), `config`, `router`, `server` |
| `src/c_api.rs` | C ABI exports |
| `include/mma.h` | C header |
| `examples/main_server.rs`, `examples/main_server.c` | Rust and C server examples |
| `tests/` | configuration tests (codec tests are currently disabled) |
