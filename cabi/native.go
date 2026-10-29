//go:build cgo

package cabi

/*
#cgo CFLAGS: -I${SRCDIR}/../include
#cgo linux LDFLAGS: -L${SRCDIR}/../target/release -lMMA
#cgo darwin LDFLAGS: -L${SRCDIR}/../target/release -lMMA
#cgo windows LDFLAGS: -L${SRCDIR}/../target/release -L${SRCDIR}/../target/x86_64-pc-windows-gnu/release -lMMA

#include "bridge.h"
#include <stdlib.h>
#include <stdint.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"runtime/cgo"
	"sync"
	"time"
	"unicode/utf8"
	"unsafe"
)

// DefaultServerConfig returns the configuration supplied by the native library.
func DefaultServerConfig() (ServerConfig, error) {
	var native C.MMA_ServerConfig
	if err := statusError(int32(C.mma_server_config_default(&native))); err != nil {
		return ServerConfig{}, err
	}
	return serverConfigFromC(native), nil
}

// DefaultFramerConfig returns the framer settings from the native server defaults.
// The current C ABI exposes FramerConfig only as part of MMA_ServerConfig.
func DefaultFramerConfig() (FramerConfig, error) {
	config, err := DefaultServerConfig()
	if err != nil {
		return FramerConfig{}, err
	}
	return config.Framer, nil
}

func serverConfigFromC(native C.MMA_ServerConfig) ServerConfig {
	bindIP := ""
	if native.bind_ip != nil {
		bindIP = C.GoString(native.bind_ip)
	}
	return ServerConfig{
		BindIP:         bindIP,
		Port:           uint16(native.port),
		MaxBatch:       uint32(native.max_batch),
		MaxInFlight:    uint32(native.max_in_flight),
		Framer:         framerConfigFromC(native.framer),
		MaxConnections: uint32(native.max_connections),
		DrainTimeout:   time.Duration(native.drain_timeout_ms) * time.Millisecond,
		ShutdownGrace:  time.Duration(native.shutdown_grace_ms) * time.Millisecond,
		RequestTimeout: time.Duration(native.request_timeout_ms) * time.Millisecond,
	}
}

func framerConfigFromC(native C.MMA_FramerConfig) FramerConfig {
	return FramerConfig{
		MaxMessageLength: uint32(native.max_message_length),
		OpcodePos:        uint8(native.opcode_pos),
		VersionPos:       uint8(native.version_pos),
		RouteLenPos:      uint8(native.route_len_pos),
		RouteOrder:       uint8(native.route_order),
		PayloadOrder:     uint8(native.payload_order),
		OptionsOrder:     uint8(native.options_order),
		OptionsCountPos:  uint8(native.options_count_pos),
		OptionsKeyFirst:  native.options_key_first != 0,
		HeaderLength:     uint8(native.header_length),
		BigBytesConfig: BigBytesConfig{
			OptionsLenOrder: uint8(native.big_bytes_config.options_len_order),
			PayloadLenOrder: uint8(native.big_bytes_config.payload_len_order),
			ReqIDOrder:      uint8(native.big_bytes_config.req_id_order),
		},
	}
}

func framerConfigToC(config FramerConfig) C.MMA_FramerConfig {
	return C.MMA_FramerConfig{
		max_message_length: C.uint32_t(config.MaxMessageLength),
		opcode_pos:         C.uint8_t(config.OpcodePos),
		version_pos:        C.uint8_t(config.VersionPos),
		route_len_pos:      C.uint8_t(config.RouteLenPos),
		route_order:        C.uint8_t(config.RouteOrder),
		payload_order:      C.uint8_t(config.PayloadOrder),
		options_order:      C.uint8_t(config.OptionsOrder),
		options_count_pos:  C.uint8_t(config.OptionsCountPos),
		options_key_first:  boolToCUint8(config.OptionsKeyFirst),
		header_length:      C.uint8_t(config.HeaderLength),
		big_bytes_config: C.MMA_BigBytesConfig{
			options_len_order: C.uint8_t(config.BigBytesConfig.OptionsLenOrder),
			payload_len_order: C.uint8_t(config.BigBytesConfig.PayloadLenOrder),
			req_id_order:      C.uint8_t(config.BigBytesConfig.ReqIDOrder),
		},
	}
}

func serverConfigToC(config ServerConfig) (C.MMA_ServerConfig, *C.char, error) {
	if config.BindIP == "" {
		return C.MMA_ServerConfig{}, nil, fmt.Errorf("%w: BindIP must not be empty", ErrInvalidArgument)
	}
	if config.DrainTimeout < 0 || config.ShutdownGrace < 0 || config.RequestTimeout < 0 {
		return C.MMA_ServerConfig{}, nil, fmt.Errorf("%w: timeouts must not be negative", ErrInvalidConfig)
	}
	bindIP := C.CString(config.BindIP)
	native := C.MMA_ServerConfig{
		bind_ip:            bindIP,
		port:               C.uint16_t(config.Port),
		max_batch:          C.uint32_t(config.MaxBatch),
		max_in_flight:      C.uint32_t(config.MaxInFlight),
		framer:             framerConfigToC(normalizeFramerConfig(config.Framer)),
		max_connections:    C.uint32_t(config.MaxConnections),
		drain_timeout_ms:   C.uint64_t(config.DrainTimeout / time.Millisecond),
		shutdown_grace_ms:  C.uint64_t(config.ShutdownGrace / time.Millisecond),
		request_timeout_ms: C.uint64_t(config.RequestTimeout / time.Millisecond),
	}
	return native, bindIP, nil
}

func boolToCUint8(value bool) C.uint8_t {
	if value {
		return 1
	}
	return 0
}

// Server owns an MMA_Server allocated by the Rust library.
type Server struct {
	mu          sync.Mutex
	lifecycleMu sync.Mutex
	ptr         *C.MMA_Server
	handles     []cgo.Handle
	stopping    bool
}

func NewServer(config ServerConfig) (*Server, error) {
	nativeConfig, bindIP, err := serverConfigToC(config)
	if err != nil {
		return nil, err
	}
	defer C.free(unsafe.Pointer(bindIP))

	var ptr *C.MMA_Server
	if err := statusError(int32(C.mma_server_create(&nativeConfig, &ptr))); err != nil {
		return nil, err
	}
	if ptr == nil {
		return nil, errors.New("mma_server_create returned a nil pointer")
	}
	server := &Server{ptr: ptr}
	runtime.SetFinalizer(server, (*Server).finalize)
	return server, nil
}

func (s *Server) finalize() { _ = s.Close() }

// RegisterRoute registers a one-shot route. Register routes before Start.
func (s *Server) RegisterRoute(route string, handler RouteHandler) error {
	if s == nil {
		return ErrClosed
	}
	if handler == nil || len(route) == 0 || len(route) > 255 || !utf8.ValidString(route) {
		return ErrInvalidArgument
	}
	handle := cgo.NewHandle(handler)
	routeBytes := []byte(route)
	routePtr := C.CBytes(routeBytes)
	defer C.free(routePtr)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == nil {
		handle.Delete()
		return ErrClosed
	}
	if s.stopping {
		handle.Delete()
		return ErrServer
	}
	status := C.mma_server_register_go_route(
		s.ptr,
		(*C.uint8_t)(routePtr),
		C.size_t(len(routeBytes)),
		C.mma_handle_to_ptr(C.uintptr_t(handle)),
	)
	if err := statusError(int32(status)); err != nil {
		handle.Delete()
		return err
	}
	s.handles = append(s.handles, handle)
	return nil
}

// Handle is an alias for RegisterRoute.
func (s *Server) Handle(route string, handler RouteHandler) error {
	return s.RegisterRoute(route, handler)
}

// RegisterChannelHandler registers a handler for messages on a channel route.
func (s *Server) RegisterChannelHandler(route string, handler ChannelHandler) error {
	if s == nil {
		return ErrClosed
	}
	if handler == nil || len(route) == 0 || len(route) > 255 || !utf8.ValidString(route) {
		return ErrInvalidArgument
	}
	handle := cgo.NewHandle(handler)
	routeBytes := []byte(route)
	routePtr := C.CBytes(routeBytes)
	defer C.free(routePtr)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == nil {
		handle.Delete()
		return ErrClosed
	}
	if s.stopping {
		handle.Delete()
		return ErrServer
	}
	status := C.mma_server_register_go_channel_handler(
		s.ptr,
		(*C.uint8_t)(routePtr),
		C.size_t(len(routeBytes)),
		C.mma_handle_to_ptr(C.uintptr_t(handle)),
	)
	if err := statusError(int32(status)); err != nil {
		handle.Delete()
		return err
	}
	s.handles = append(s.handles, handle)
	return nil
}

func (s *Server) Start() error {
	if s == nil {
		return ErrClosed
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == nil {
		return ErrClosed
	}
	return statusError(int32(C.mma_server_start(s.ptr)))
}

// BoundPort returns the configured/listener port reported by the native server.
func (s *Server) BoundPort() (uint16, error) {
	if s == nil {
		return 0, ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == nil {
		return 0, ErrClosed
	}
	if s.stopping {
		return 0, ErrServer
	}
	var port C.uint16_t
	if err := statusError(int32(C.mma_server_local_port(s.ptr, &port))); err != nil {
		return 0, err
	}
	return uint16(port), nil
}

// GetHandle returns a separately owned handle that can send channel messages
// and change the running server's configuration.
func (s *Server) GetHandle() (*ServerHandle, error) {
	if s == nil {
		return nil, ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == nil {
		return nil, ErrClosed
	}
	if s.stopping {
		return nil, ErrServer
	}
	var ptr *C.MMA_ServerHandle
	if err := statusError(int32(C.mma_server_get_handle(s.ptr, &ptr))); err != nil {
		return nil, err
	}
	if ptr == nil {
		return nil, errors.New("mma_server_get_handle returned a nil pointer")
	}
	handle := &ServerHandle{ptr: ptr}
	runtime.SetFinalizer(handle, (*ServerHandle).finalize)
	return handle, nil
}

// Stop stops the native server but keeps the wrapper allocated until Close.
// The native API does not support starting the same server instance again.
func (s *Server) Stop() error {
	if s == nil {
		return nil
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	ptr := s.ptr
	if ptr == nil {
		s.mu.Unlock()
		return nil
	}
	s.stopping = true
	s.mu.Unlock()

	err := statusError(int32(C.mma_server_stop(ptr)))
	s.mu.Lock()
	s.stopping = false
	s.mu.Unlock()
	return err
}

// Close releases the server and its registered Go callback handles.
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.mu.Lock()
	ptr := s.ptr
	s.ptr = nil
	s.stopping = true
	handles := s.handles
	s.handles = nil
	s.mu.Unlock()
	runtime.SetFinalizer(s, nil)
	if ptr == nil {
		return nil
	}
	// Native destroy stops and joins the server thread before callbacks are released.
	C.mma_server_destroy(ptr)
	for _, handle := range handles {
		handle.Delete()
	}
	return nil
}

// ServerHandle is an independently owned handle for runtime server operations.
type ServerHandle struct {
	mu  sync.Mutex
	ptr *C.MMA_ServerHandle
}

func (h *ServerHandle) finalize() { _ = h.Close() }

func (h *ServerHandle) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	ptr := h.ptr
	h.ptr = nil
	h.mu.Unlock()
	runtime.SetFinalizer(h, nil)
	if ptr != nil {
		C.mma_server_handle_destroy(ptr)
	}
	return nil
}

// SendToChannel sends a message to a connected channel client.
func (h *ServerHandle) SendToChannel(connectionID uint32, message Response) error {
	if h == nil {
		return ErrClosed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ptr == nil {
		return ErrClosed
	}
	response := C.mma_response_new(C.uint32_t(message.ReqID))
	if response.inner == nil {
		return errors.New("mma_response_new returned a nil response")
	}
	defer C.mma_response_destroy(response)

	opcode := message.Opcode
	if opcode == 0 {
		opcode = StatusMessage
	}
	response = C.mma_response_set_status(response, C.uint8_t(opcode))
	if response.inner == nil {
		return fmt.Errorf("%w: unsupported response opcode %d", ErrInvalidArgument, opcode)
	}
	response, err := setNativeResponsePayload(response, message.Payload)
	if err != nil {
		return err
	}
	for _, option := range message.Options {
		response, err = addNativeResponseOption(response, option.Key, option.Value)
		if err != nil {
			return err
		}
	}
	return statusError(int32(C.mma_server_handle_send_to_channel(h.ptr, C.uint32_t(connectionID), response)))
}

// ChangeConfig applies a new configuration to the running server.
func (h *ServerHandle) ChangeConfig(config ServerConfig) error {
	if h == nil {
		return ErrClosed
	}
	nativeConfig, bindIP, err := serverConfigToC(config)
	if err != nil {
		return err
	}
	defer C.free(unsafe.Pointer(bindIP))
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ptr == nil {
		return ErrClosed
	}
	return statusError(int32(C.mma_server_handle_change_config(h.ptr, &nativeConfig)))
}

// ResponseBuilder mutates the native response for a route callback. It is only
// valid for the duration of RouteHandler.
type ResponseBuilder struct {
	ptr *C.MMA_Response
}

// SetStatus accepts an optional legacy version argument. The native ABI no
// longer exposes a response-version setter, so version is ignored.
func (b *ResponseBuilder) SetStatus(opcode ResponseOpcode, _ ...uint8) error {
	if b == nil || b.ptr == nil || b.ptr.inner == nil {
		return ErrInvalidArgument
	}
	updated := C.mma_response_set_status(*b.ptr, C.uint8_t(opcode))
	if updated.inner == nil {
		return fmt.Errorf("%w: unsupported response opcode %d", ErrInvalidArgument, opcode)
	}
	*b.ptr = updated
	return nil
}

func (b *ResponseBuilder) SetStatusBuilder(opcode ResponseOpcode, version ...uint8) error {
	return b.SetStatus(opcode, version...)
}

func (b *ResponseBuilder) SetPayload(payload []byte) error {
	if b == nil || b.ptr == nil || b.ptr.inner == nil {
		return ErrInvalidArgument
	}
	updated, err := setNativeResponsePayload(*b.ptr, payload)
	if err != nil {
		return err
	}
	*b.ptr = updated
	return nil
}

func (b *ResponseBuilder) AddOption(key, value string) error {
	if b == nil || b.ptr == nil || b.ptr.inner == nil {
		return ErrInvalidArgument
	}
	updated, err := addNativeResponseOption(*b.ptr, key, value)
	if err != nil {
		return err
	}
	*b.ptr = updated
	return nil
}

func setNativeResponsePayload(response C.MMA_Response, payload []byte) (C.MMA_Response, error) {
	var ptr unsafe.Pointer
	if len(payload) > 0 {
		ptr = C.CBytes(payload)
		defer C.free(ptr)
	}
	updated := C.mma_response_set_payload(response, (*C.uint8_t)(ptr), C.size_t(len(payload)))
	if updated.inner == nil {
		return response, ErrInvalidArgument
	}
	return updated, nil
}

func addNativeResponseOption(response C.MMA_Response, key, value string) (C.MMA_Response, error) {
	if key == "" || value == "" {
		return response, fmt.Errorf("%w: option key and value must not be empty", ErrInvalidArgument)
	}
	keyPtr := C.CBytes([]byte(key))
	valuePtr := C.CBytes([]byte(value))
	defer C.free(keyPtr)
	defer C.free(valuePtr)
	updated := C.mma_response_add_option(
		response,
		(*C.uint8_t)(keyPtr), C.size_t(len(key)),
		(*C.uint8_t)(valuePtr), C.size_t(len(value)),
	)
	if updated.inner == nil {
		return response, ErrInvalidArgument
	}
	return updated, nil
}

func requestFromC(native *C.MMA_Request) (*Request, error) {
	if native == nil {
		return nil, ErrInvalidArgument
	}
	routeBytes, err := copyFromC(native.route, native.route_len)
	if err != nil {
		return nil, err
	}
	payload, err := copyFromC(native.payload, native.payload_len)
	if err != nil {
		return nil, err
	}
	count, err := cSizeToInt(native.options_count)
	if err != nil || count > 255 {
		return nil, ErrInvalidArgument
	}
	if count > 0 && native.options == nil {
		return nil, ErrInvalidArgument
	}
	options := make([]Option, 0, count)
	if count > 0 {
		nativeOptions := unsafe.Slice(native.options, count)
		for _, option := range nativeOptions {
			key, err := copyFromC(option.key, option.key_len)
			if err != nil {
				return nil, err
			}
			value, err := copyFromC(option.value, option.value_len)
			if err != nil {
				return nil, err
			}
			options = append(options, Option{Key: string(key), Value: string(value)})
		}
	}
	return &Request{
		Opcode:  RequestOpcode(native.opcode),
		Version: uint8(native.version),
		ReqID:   uint32(native.request_id),
		Route:   string(routeBytes),
		Options: options,
		Payload: payload,
	}, nil
}

func copyFromC(ptr *C.uint8_t, size C.size_t) ([]byte, error) {
	n, err := cSizeToInt(size)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	if ptr == nil {
		return nil, ErrInvalidArgument
	}
	source := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), n)
	result := make([]byte, n)
	copy(result, source)
	return result, nil
}

func cSizeToInt(size C.size_t) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if uint64(size) > uint64(maxInt) {
		return 0, errors.New("C size_t value does not fit into Go int")
	}
	return int(size), nil
}

func setCallbackError(response *C.MMA_Response, opcode ResponseOpcode, message string) {
	if response == nil || response.inner == nil {
		return
	}
	builder := &ResponseBuilder{ptr: response}
	if err := builder.SetStatus(opcode); err != nil {
		return
	}
	_ = builder.SetPayload([]byte(message))
}

//export go_mma_route_callback
func go_mma_route_callback(request *C.MMA_Request, response *C.MMA_Response, userData unsafe.Pointer) (status C.int32_t) {
	status = C.int32_t(C.MMA_OK)
	defer func() {
		if recover() != nil {
			setCallbackError(response, StatusInternalError, "Internal server error")
			status = C.int32_t(C.MMA_OK)
		}
	}()
	if response == nil || response.inner == nil {
		return C.int32_t(C.MMA_INVALID_ARGUMENT)
	}
	handleValue := uintptr(C.mma_ptr_to_handle(userData))
	if handleValue == 0 {
		setCallbackError(response, StatusInternalError, "Internal server error")
		return status
	}
	handler, ok := cgo.Handle(handleValue).Value().(RouteHandler)
	if !ok || handler == nil {
		setCallbackError(response, StatusInternalError, "Internal server error")
		return status
	}
	goRequest, err := requestFromC(request)
	if err != nil {
		setCallbackError(response, StatusBadRequest, "Invalid request")
		return status
	}
	builder := &ResponseBuilder{ptr: response}
	if err := handler(goRequest, builder); err != nil {
		setCallbackError(response, StatusInternalError, "Internal server error")
	}
	return status
}

//export go_mma_channel_callback
func go_mma_channel_callback(request *C.MMA_Request, connectionID C.uint32_t, userData unsafe.Pointer) (status C.int32_t) {
	status = C.int32_t(C.MMA_SERVER_ERROR)
	defer func() {
		if recover() != nil {
			status = C.int32_t(C.MMA_SERVER_ERROR)
		}
	}()
	if request == nil {
		return C.int32_t(C.MMA_INVALID_ARGUMENT)
	}
	handleValue := uintptr(C.mma_ptr_to_handle(userData))
	if handleValue == 0 {
		return C.int32_t(C.MMA_INVALID_ARGUMENT)
	}
	handler, ok := cgo.Handle(handleValue).Value().(ChannelHandler)
	if !ok || handler == nil {
		return C.int32_t(C.MMA_INVALID_ARGUMENT)
	}
	goRequest, err := requestFromC(request)
	if err != nil {
		return C.int32_t(C.MMA_INVALID_ARGUMENT)
	}
	if err := handler(goRequest, uint32(connectionID)); err != nil {
		return C.int32_t(C.MMA_SERVER_ERROR)
	}
	return C.int32_t(C.MMA_OK)
}

// normalizeFramerConfig fills the default ordering when callers leave only the
// nested big-bytes ordering unspecified. The rest of the configuration is left
// unchanged so the native validator can report invalid configurations.
func normalizeFramerConfig(config FramerConfig) FramerConfig {
	if config.BigBytesConfig == (BigBytesConfig{}) {
		config.BigBytesConfig = BigBytesConfig{
			OptionsLenOrder: 1,
			PayloadLenOrder: 2,
			ReqIDOrder:      3,
		}
	}
	return config
}
