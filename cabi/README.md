# Go bindings for the MMA Rust C ABI

Package `cabi` is a thin Go wrapper around the functions already exported by
`include/mma.h`. The protocol engine and server remain in Rust. The wrapper
only converts configuration/request/response types, owns C pointers, and bridges
callbacks from Rust to Go using a C trampoline and `runtime/cgo.Handle`.

## Native requirement

The implementation requires cgo, a compatible C compiler, and the built Rust
shared library. With `CGO_ENABLED=0`, `native_nocgo.go` keeps IDE/type checking
working but every native operation returns `ErrCGODisabled`; it is not a Go
fallback implementation.

See [`../INSTALL.txt`](../INSTALL.txt) for Windows, Linux and macOS build steps.

## Example

```go
package main

import (
    "log"

    "github.com/Edestuss0/MMA-Network-protocol/cabi"
)

func main() {
    cfg, err := cabi.DefaultServerConfig()
    if err != nil {
        log.Fatal(err)
    }
    cfg.BindIP = "127.0.0.1"
    cfg.Port = 9000

    server, err := cabi.NewServer(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer server.Close()

    if err := server.Handle("ping", func(req *cabi.Request, res *cabi.ResponseBuilder) error {
        if err := res.SetStatus(cabi.StatusOK); err != nil {
            return err
        }
        return res.SetPayload([]byte("pong"))
    }); err != nil {
        log.Fatal(err)
    }

    if err := server.Start(); err != nil {
        log.Fatal(err)
    }
    select {}
}
```

## Wrapped C ABI operations

- Native default config and server creation/start/stop/destroy.
- Route and channel-handler registration.
- Native response status/payload/options operations.
- `MMA_ServerHandle` retrieval, channel send and runtime config update.

The current C ABI does not export standalone framer create/encode/decode
functions. `FramerConfig` is passed through the native server configuration;
no protocol framer is reimplemented in Go.
