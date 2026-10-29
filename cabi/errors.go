package cabi

import (
	"errors"
	"fmt"
)

var (
	ErrIncomplete      = errors.New("frame is incomplete")
	ErrInvalidArgument = errors.New("invalid argument")
	ErrInvalidConfig   = errors.New("invalid config")
	ErrEncode          = errors.New("encode failed")
	ErrDecode          = errors.New("decode failed")
	ErrServer          = errors.New("server operation failed")
	ErrServerRunning   = errors.New("server is already running")
	ErrCallback        = errors.New("route callback failed")
	ErrClosed          = errors.New("object is closed")
	ErrCGODisabled     = errors.New("MMA C ABI requires cgo; enable CGO_ENABLED=1 and install a C compiler")
)

type StatusError struct {
	Code    int32
	Message string
}

func (e *StatusError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" {
		return fmt.Sprintf("MMA C ABI status %d", e.Code)
	}
	return fmt.Sprintf("MMA C ABI status %d: %s", e.Code, e.Message)
}

func (e *StatusError) Unwrap() error {
	if e == nil {
		return nil
	}
	switch e.Code {
	case -1:
		return ErrInvalidArgument
	case -2:
		return ErrInvalidConfig
	case -3:
		return ErrServer
	case -4:
		return ErrServerRunning
	default:
		return nil
	}
}

func statusError(code int32) error {
	if code == 0 {
		return nil
	}
	message := "unknown status"
	switch code {
	case -1:
		message = "invalid argument"
	case -2:
		message = "invalid configuration"
	case -3:
		message = "server operation failed"
	case -4:
		message = "server is already started"
	}
	return &StatusError{Code: code, Message: message}
}
