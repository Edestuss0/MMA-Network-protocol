# MMA Go C ABI layer

This directory is the low-level Go adapter for the Rust `MMA` library exported through `include/mma.h`.

## Files

- `cgo.go` — all cgo/FFI calls, memory conversions, request decoding, response building, server callbacks and server lifecycle.
- `types.go` — public Go protocol types.
- `errors.go` — C status -> Go error mapping.
- `mma.h` — corrected C header. Important: it includes `max_in_flight` in `MMAServerConfig`, matching the current Rust `src/c_api.rs`.

## Intended architecture

Rust -> C ABI -> `cgo.go` -> higher-level Go package.

The higher-level package should not expose `C.*`, `unsafe.Pointer`, `MMARequest`, `MMAFramer`, or `MMAServer` directly.

## Current coverage

- default framer config
- framer creation/destruction
- incremental frame decode
- response encode
- request metadata / route / payload / options
- server config
- server create/start/stop/destroy
- bound port
- route registration
- Rust -> C -> Go callback trampoline
- response builder: status / payload / options
- C/Rust-owned memory cleanup
- `runtime/cgo.Handle` for callback userdata

## Callback rule

A `RouteHandler` is synchronous. Do not call `Start`, `Stop`, `Close`, or `RegisterRoute` on the same server from inside its own callback. The Rust C ABI server waits for callbacks during stop/destroy, so such re-entrant lifecycle calls can deadlock at the protocol level.

## Build

The Go package expects the Rust library and header at:

`MMA-Network-protocol/include/mma.h`

`MMA-Network-protocol/target/release/libMMA.so` / `libMMA.a`

and currently uses:

`-L${SRCDIR}/MMA-Network-protocol/target/release -lMMA`

## Example high-level usage

```go
config, err := cabi.DefaultServerConfig()
if err != nil {
    return err
}

server, err := cabi.NewServer(config)
if err != nil {
    return err
}
defer server.Close()

err = server.Handle("GET", func(req *cabi.Request, res *cabi.ResponseBuilder) error {
    if err := res.SetStatus(cabi.StatusOK, req.Version); err != nil {
        return err
    }
    if err := res.AddOption("content-type", "application/json"); err != nil {
        return err
    }
    return res.SetPayload([]byte(`{"ok":true}`))
})
if err != nil {
    return err
}

return server.Start()
```

The future higher-level package can turn this primitive route API into whatever public API you choose: REST-like resources, middleware, JSON encoding, authentication, etc.
