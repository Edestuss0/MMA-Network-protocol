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
)

type StatusError struct {
	Code    int32
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("C ABI status %d: %s", e.Code, e.Message)
}

func (e *StatusError) Unwrap() error {
	switch e.Code {
	case 1:
		return ErrIncomplete
	case -1:
		return ErrInvalidArgument
	case -2:
		return ErrInvalidConfig
	case -3:
		return ErrEncode
	case -4:
		return ErrDecode
	case -5:
		return ErrServer
	case -6:
		return ErrServerRunning
	case -7:
		return ErrCallback
	default:
		return nil
	}
}
