package cabi

import (
	"errors"
	"testing"
	"unsafe"
)

func TestStatusErrorUnwrap(t *testing.T) {
	cases := []struct {
		code int32
		want error
	}{
		{-1, ErrInvalidArgument},
		{-2, ErrInvalidConfig},
		{-3, ErrServer},
		{-4, ErrServerRunning},
	}
	for _, test := range cases {
		err := &StatusError{Code: test.code, Message: "test"}
		if !errors.Is(err, test.want) {
			t.Errorf("errors.Is(%v, %v) = false", err, test.want)
		}
	}
}

func TestNativeStructLayout64(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) != 8 {
		t.Skip("layout assertions are for 64-bit targets")
	}
	checks := []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"MMA_BigBytesConfig", unsafe.Sizeof(nativeBigBytesConfig{}), 3},
		{"MMA_FramerConfig", unsafe.Sizeof(nativeFramerConfig{}), 16},
		{"MMA_ServerConfig", unsafe.Sizeof(nativeServerConfig{}), 64},
		{"MMA_Option", unsafe.Sizeof(nativeOption{}), 32},
		{"MMA_Request", unsafe.Sizeof(nativeRequest{}), 56},
		{"MMA_Response", unsafe.Sizeof(nativeResponse{}), 8},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s size = %d, want %d", check.name, check.got, check.want)
		}
	}
}
