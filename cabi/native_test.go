//go:build native_integration

package cabi

import (
	"testing"
	"time"
)

func TestDefaultServerConfigMirrorsNativeDefaults(t *testing.T) {
	config, err := DefaultServerConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.BindIP != "0.0.0.0" || config.Port != 8080 {
		t.Fatalf("unexpected bind defaults: %s:%d", config.BindIP, config.Port)
	}
	if config.MaxBatch != 16 || config.MaxInFlight != 1024 || config.MaxConnections != 128000 {
		t.Fatalf("unexpected concurrency defaults: %+v", config)
	}
	if config.DrainTimeout != 2*time.Second || config.ShutdownGrace != 10*time.Second || config.RequestTimeout != 10*time.Second {
		t.Fatalf("unexpected timeout defaults: %+v", config)
	}
	if config.Framer.HeaderLength != 29 || config.Framer.BigBytesConfig != (BigBytesConfig{OptionsLenOrder: 1, PayloadLenOrder: 2, ReqIDOrder: 3}) {
		t.Fatalf("unexpected framer defaults: %+v", config.Framer)
	}
}
