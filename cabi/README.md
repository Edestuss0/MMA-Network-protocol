# Go bindings for the MMA Rust C ABI

`github.com/Edestuss0/MMA-Network-protocol/cabi` is a Go wrapper around the existing native API in `include/mma.h`. The Rust protocol engine and its C ABI are not reimplemented or modified by the Go binding.

## Consumer requirements

- Go 1.25 or later.
- A supported OS/architecture: Windows amd64, Linux amd64, macOS amd64 or arm64.
- Internet access on the first native API call, so the matching prebuilt Rust shared library and callback bridge can be downloaded from the GitHub release and cached. No Rust, Cargo, GCC, MinGW, MSVC, or CGO is required by the application using this package.

The native artifact is selected from the dependency's module version when available. For local development, you can set `MMA_NATIVE_VERSION` to a release tag or `MMA_NATIVE_DIR` to a directory that contains both the native library and matching callback bridge (`MMA.dll` + `mma_go_bridge.dll`, `libMMA.so` + `libmma_go_bridge.so`, or `libMMA.dylib` + `libmma_go_bridge.dylib`). Set `MMA_NATIVE_REFRESH=1` before starting the process to redownload the cached artifact. The artifact zip and its SHA-256 checksum are verified before installation.

The maintainer workflow [`.github/workflows/native-release.yml`](../.github/workflows/native-release.yml) builds the native libraries and pointer-based callback bridge on GitHub Actions and attaches them to a version release. The bridge adapts the C route callback that passes structs by value; the Rust C ABI and `include/mma.h` remain unchanged. Push a tag such as `v0.1.0` to run it. Until a release with the corresponding native assets exists, consumers cannot initialize the native API from GitHub; use `MMA_NATIVE_DIR` for local testing.

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

- Native defaults and server creation/start/stop/destroy.
- Route and channel-handler registration.
- Native response status/payload/options operations.
- `MMA_ServerHandle` retrieval, channel sends and runtime configuration changes.

The current C ABI does not export standalone framer encode/decode functions. `FramerConfig` is passed through the native server configuration; framing remains entirely inside Rust. Go calls the unmodified C ABI using `purego`. The route callback passes through a prebuilt C bridge because purego callbacks cannot receive/return C structs by value on Windows; the bridge uses pointer arguments on the Go side.

## Tests

`go test ./cabi` runs Go-side tests without downloading native binaries. Native integration test(s) are opt-in:

```powershell
$env:MMA_NATIVE_DIR = "D:\path\to\directory\with\both\libraries"
go test -tags native_integration ./cabi
```
