package cabi

/*
#cgo linux CFLAGS: -I${SRCDIR}/MMA-Network-protocol/include
#cgo linux LDFLAGS: -L${SRCDIR}/MMA-Network-protocol/target/release -lMMA

#include "mma.h"
#include <stdlib.h>
#include <stdint.h>

// cgo cannot pass a Go function directly as a C function pointer.
// We therefore use a C trampoline: Rust -> C trampoline -> exported Go function.
extern int32_t go_mma_route_callback(
    MMARequest *request,
    MMARouteResponse *response,
    void *user_data
);

static int32_t mma_go_route_callback(
    const MMARequest *request,
    MMARouteResponse *response,
    void *user_data
) {
    return go_mma_route_callback((MMARequest *)request, response, user_data);
}

// cgo.Handle is a uintptr-sized integer. Store it in the C void* field.
static void *mma_handle_to_ptr(uintptr_t handle) {
    return (void *)handle;
}

static uintptr_t mma_ptr_to_handle(void *ptr) {
    return (uintptr_t)ptr;
}

static int32_t mma_server_register_go_route(
    MMAServer *server,
    const uint8_t *route,
    size_t route_len,
    void *user_data
) {
    return mma_server_register_route(
        server,
        route,
        route_len,
        mma_go_route_callback,
        user_data
    );
}
*/
import "C"

import (
	"errors"
	"runtime"
	"runtime/cgo"
	"sync"
	"unsafe"
)

// Framer wraps the Rust MMAFramer handle.
// MMAFramer is an opaque C type: its internal layout is deliberately hidden.
type Framer struct {
	mu  sync.Mutex
	ptr *C.MMAFramer
}

func NewFramer(config FramerConfig) (*Framer, error) {
	c := C.MMAFramerConfig{
		max_message_length: C.uint32_t(config.MaxMessageLength),
		opcode_pos:         C.uint8_t(config.OpcodePos),
		version_pos:        C.uint8_t(config.VersionPos),
		route_len_pos:      C.uint8_t(config.RouteLenPos),
		route_order:        C.uint8_t(config.RouteOrder),
		payload_order:      C.uint8_t(config.PayloadOrder),
		options_order:      C.uint8_t(config.OptionsOrder),
		options_count_pos:  C.uint8_t(config.OptionsCountPos),
		options_key_first:  0,
		header_length:      C.uint8_t(config.HeaderLength),
	}

	if config.OptionsKeyFirst {
		c.options_key_first = 1
	}

	var ptr *C.MMAFramer
	status := C.mma_framer_create(&c, &ptr)
	if status != C.MMA_STATUS_OK {
		return nil, statusError(status)
	}
	if ptr == nil {
		return nil, errors.New("mma_framer_create returned nil pointer")
	}

	f := &Framer{ptr: ptr}
	runtime.SetFinalizer(f, (*Framer).finalize)
	return f, nil
}

func DefaultFramerConfig() (FramerConfig, error) {
	var c C.MMAFramerConfig
	status := C.mma_framer_config_default(&c)
	if status != C.MMA_STATUS_OK {
		return FramerConfig{}, statusError(status)
	}

	return FramerConfig{
		MaxMessageLength: uint32(c.max_message_length),
		OpcodePos:        uint8(c.opcode_pos),
		VersionPos:       uint8(c.version_pos),
		RouteLenPos:      uint8(c.route_len_pos),
		RouteOrder:       uint8(c.route_order),
		PayloadOrder:     uint8(c.payload_order),
		OptionsOrder:     uint8(c.options_order),
		OptionsCountPos:  uint8(c.options_count_pos),
		OptionsKeyFirst:  c.options_key_first != 0,
		HeaderLength:     uint8(c.header_length),
	}, nil
}

func (f *Framer) finalize() {
	_ = f.Close()
}

func (f *Framer) Close() error {
	if f == nil {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ptr == nil {
		return nil
	}

	C.mma_framer_destroy(f.ptr)
	f.ptr = nil
	runtime.SetFinalizer(f, nil)
	return nil
}

// Encode converts a Go Response into a Rust-owned encoded frame.
func (f *Framer) Encode(response Response) ([]byte, error) {
	if f == nil {
		return nil, ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ptr == nil {
		return nil, ErrClosed
	}

	payloadPtr := cBytes(response.Payload)
	if payloadPtr != nil {
		defer C.free(payloadPtr)
	}

	optionsPtr, optionsMemory, err := makeCOptions(response.Options)
	if err != nil {
		return nil, err
	}
	defer freeCOptions(optionsPtr, optionsMemory)

	cResponse := C.MMAResponse{
		opcode:        C.uint8_t(response.Opcode),
		version:       C.uint8_t(response.Version),
		req_id:        C.uint32_t(response.ReqID),
		payload:       (*C.uint8_t)(payloadPtr),
		payload_len:   C.size_t(len(response.Payload)),
		options:       optionsPtr,
		options_count: C.size_t(len(response.Options)),
	}

	var output C.MMABytes
	status := C.mma_response_encode(f.ptr, &cResponse, &output)
	if status != C.MMA_STATUS_OK {
		if output.data != nil {
			C.mma_bytes_free(output)
		}
		return nil, statusError(status)
	}

	result, err := copyFromC(output.data, output.len)
	C.mma_bytes_free(output)
	return result, err
}

// Decode incrementally feeds bytes into the stateful Rust framer.
func (f *Framer) Decode(data []byte) (*Request, error) {
	if f == nil {
		return nil, ErrClosed
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.ptr == nil {
		return nil, ErrClosed
	}

	var requestPtr *C.MMARequest
	var dataPtr *C.uint8_t
	if len(data) > 0 {
		dataPtr = (*C.uint8_t)(unsafe.Pointer(&data[0]))
	}

	status := C.mma_framer_decode(
		f.ptr,
		dataPtr,
		C.size_t(len(data)),
		&requestPtr,
	)

	runtime.KeepAlive(data)

	if requestPtr != nil {
		defer C.mma_request_destroy(requestPtr)
	}

	switch status {
	case C.MMA_STATUS_OK:
		// Continue below.
	case C.MMA_STATUS_INCOMPLETE:
		return nil, ErrIncomplete
	default:
		return nil, statusError(status)
	}

	return requestFromC(requestPtr)
}

func requestFromC(ptr *C.MMARequest) (*Request, error) {
	if ptr == nil {
		return nil, ErrInvalidArgument
	}

	var opcode C.uint8_t
	var version C.uint8_t
	var reqID C.uint32_t

	status := C.mma_request_metadata(ptr, &opcode, &version, &reqID)
	if status != C.MMA_STATUS_OK {
		return nil, statusError(status)
	}

	route, err := requestRouteFromC(ptr)
	if err != nil {
		return nil, err
	}

	payload, err := requestPayloadFromC(ptr)
	if err != nil {
		return nil, err
	}

	options, err := requestOptionsFromC(ptr)
	if err != nil {
		return nil, err
	}

	return &Request{
		Opcode:  RequestOpcode(uint8(opcode)),
		Version: uint8(version),
		ReqID:   uint32(reqID),
		Route:   route,
		Options: options,
		Payload: payload,
	}, nil
}

func requestRouteFromC(ptr *C.MMARequest) (string, error) {
	var data *C.uint8_t
	var size C.size_t

	status := C.mma_request_route(ptr, &data, &size)
	if status != C.MMA_STATUS_OK {
		return "", statusError(status)
	}

	value, err := copyFromC(data, size)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func requestPayloadFromC(ptr *C.MMARequest) ([]byte, error) {
	var data *C.uint8_t
	var size C.size_t

	status := C.mma_request_payload(ptr, &data, &size)
	if status != C.MMA_STATUS_OK {
		return nil, statusError(status)
	}
	return copyFromC(data, size)
}

func requestOptionsFromC(ptr *C.MMARequest) ([]Option, error) {
	var count C.size_t
	status := C.mma_request_options_count(ptr, &count)
	if status != C.MMA_STATUS_OK {
		return nil, statusError(status)
	}

	intCount, err := cSizeToInt(count)
	if err != nil {
		return nil, err
	}

	options := make([]Option, 0, intCount)
	for i := 0; i < intCount; i++ {
		var key *C.uint8_t
		var keyLen C.size_t
		var value *C.uint8_t
		var valueLen C.size_t

		status := C.mma_request_option(
			ptr,
			C.size_t(i),
			&key,
			&keyLen,
			&value,
			&valueLen,
		)
		if status != C.MMA_STATUS_OK {
			return nil, statusError(status)
		}

		keyBytes, err := copyFromC(key, keyLen)
		if err != nil {
			return nil, err
		}
		valueBytes, err := copyFromC(value, valueLen)
		if err != nil {
			return nil, err
		}

		options = append(options, Option{
			Key:   string(keyBytes),
			Value: string(valueBytes),
		})
	}

	return options, nil
}

func makeCOptions(options []Option) (*C.MMAOption, []unsafe.Pointer, error) {
	if len(options) == 0 {
		return nil, nil, nil
	}

	totalSize := C.size_t(len(options)) * C.size_t(unsafe.Sizeof(C.MMAOption{}))
	memory := C.malloc(totalSize)
	if memory == nil {
		return nil, nil, errors.New("failed to allocate C memory for options")
	}

	cOptions := unsafe.Slice((*C.MMAOption)(memory), len(options))
	buffers := make([]unsafe.Pointer, 0, len(options)*2)

	for i, option := range options {
		keyPtr := cBytes([]byte(option.Key))
		valuePtr := cBytes([]byte(option.Value))

		if keyPtr != nil {
			buffers = append(buffers, keyPtr)
		}
		if valuePtr != nil {
			buffers = append(buffers, valuePtr)
		}

		cOptions[i] = C.MMAOption{
			key:       (*C.uint8_t)(keyPtr),
			key_len:   C.size_t(len(option.Key)),
			value:     (*C.uint8_t)(valuePtr),
			value_len: C.size_t(len(option.Value)),
		}
	}

	return (*C.MMAOption)(memory), buffers, nil
}

func freeCOptions(options *C.MMAOption, buffers []unsafe.Pointer) {
	for _, ptr := range buffers {
		C.free(ptr)
	}
	if options != nil {
		C.free(unsafe.Pointer(options))
	}
}

func cBytes(data []byte) unsafe.Pointer {
	if len(data) == 0 {
		return nil
	}
	return C.CBytes(data)
}

func copyFromC(data *C.uint8_t, size C.size_t) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	if data == nil {
		return nil, ErrInvalidArgument
	}

	n, err := cSizeToInt(size)
	if err != nil {
		return nil, err
	}

	src := unsafe.Slice((*byte)(unsafe.Pointer(data)), n)
	dst := make([]byte, n)
	copy(dst, src)
	return dst, nil
}

func cSizeToInt(value C.size_t) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if uint64(value) > uint64(maxInt) {
		return 0, errors.New("C size_t value does not fit into Go int")
	}
	return int(value), nil
}

// ---------------- Server ----------------

// ResponseBuilder mutates the response that Rust will send for the current route.
// It is only valid during a RouteHandler invocation.
type ResponseBuilder struct {
	ptr *C.MMARouteResponse
}

func (b *ResponseBuilder) SetStatus(opcode ResponseOpcode, version uint8) error {
	if b == nil || b.ptr == nil {
		return ErrInvalidArgument
	}

	status := C.mma_response_set_status(
		b.ptr,
		C.uint8_t(opcode),
		C.uint8_t(version),
	)
	if status != C.MMA_STATUS_OK {
		return statusError(status)
	}
	return nil
}

func (b *ResponseBuilder) SetPayload(payload []byte) error {
	if b == nil || b.ptr == nil {
		return ErrInvalidArgument
	}

	var ptr *C.uint8_t
	if len(payload) > 0 {
		ptr = (*C.uint8_t)(unsafe.Pointer(&payload[0]))
	}

	status := C.mma_response_set_payload(
		b.ptr,
		ptr,
		C.size_t(len(payload)),
	)
	runtime.KeepAlive(payload)

	if status != C.MMA_STATUS_OK {
		return statusError(status)
	}
	return nil
}

func (b *ResponseBuilder) AddOption(key, value string) error {
	if b == nil || b.ptr == nil {
		return ErrInvalidArgument
	}

	keyBytes := []byte(key)
	valueBytes := []byte(value)

	var keyPtr *C.uint8_t
	var valuePtr *C.uint8_t
	if len(keyBytes) > 0 {
		keyPtr = (*C.uint8_t)(unsafe.Pointer(&keyBytes[0]))
	}
	if len(valueBytes) > 0 {
		valuePtr = (*C.uint8_t)(unsafe.Pointer(&valueBytes[0]))
	}

	status := C.mma_response_add_option(
		b.ptr,
		keyPtr,
		C.size_t(len(keyBytes)),
		valuePtr,
		C.size_t(len(valueBytes)),
	)
	runtime.KeepAlive(keyBytes)
	runtime.KeepAlive(valueBytes)

	if status != C.MMA_STATUS_OK {
		return statusError(status)
	}
	return nil
}

// SetStatusBuilder is an alias for users that want terminology matching the C API.
func (b *ResponseBuilder) SetStatusBuilder(opcode ResponseOpcode, version uint8) error {
	return b.SetStatus(opcode, version)
}

// RouteHandler is the Go callback invoked by the Rust server for a registered route.
type RouteHandler func(request *Request, response *ResponseBuilder) error

// ServerConfig mirrors the Rust MMAServerConfig C layout.
type ServerConfig struct {
	BindIP      string
	Port        uint16
	MaxBatch    uint8
	MaxInFlight uint32
	Framer      FramerConfig
}

// Server wraps the Rust MMA server.
type Server struct {
	mu      sync.Mutex
	ptr     *C.MMAServer
	handles []cgo.Handle
}

func DefaultServerConfig() (ServerConfig, error) {
	var c C.MMAServerConfig
	status := C.mma_server_config_default(&c)
	if status != C.MMA_STATUS_OK {
		return ServerConfig{}, statusError(status)
	}

	framer := FramerConfig{
		MaxMessageLength: uint32(c.framer.max_message_length),
		OpcodePos:        uint8(c.framer.opcode_pos),
		VersionPos:       uint8(c.framer.version_pos),
		RouteLenPos:      uint8(c.framer.route_len_pos),
		RouteOrder:       uint8(c.framer.route_order),
		PayloadOrder:     uint8(c.framer.payload_order),
		OptionsOrder:     uint8(c.framer.options_order),
		OptionsCountPos:  uint8(c.framer.options_count_pos),
		OptionsKeyFirst:  c.framer.options_key_first != 0,
		HeaderLength:     uint8(c.framer.header_length),
	}

	bindIP := ""
	if c.bind_ip != nil {
		bindIP = C.GoString(c.bind_ip)
	}

	return ServerConfig{
		BindIP:      bindIP,
		Port:        uint16(c.port),
		MaxBatch:    uint8(c.max_batch),
		MaxInFlight: uint32(c.max_in_flight),
		Framer:      framer,
	}, nil
}

func NewServer(config ServerConfig) (*Server, error) {
	if config.BindIP == "" {
		return nil, errors.New("BindIP must not be empty")
	}

	bindIP := C.CString(config.BindIP)
	defer C.free(unsafe.Pointer(bindIP))

	c := C.MMAServerConfig{
		bind_ip:   bindIP,
		port:      C.uint16_t(config.Port),
		max_batch: C.uint8_t(config.MaxBatch),
		framer: C.MMAFramerConfig{
			max_message_length: C.uint32_t(config.Framer.MaxMessageLength),
			opcode_pos:         C.uint8_t(config.Framer.OpcodePos),
			version_pos:        C.uint8_t(config.Framer.VersionPos),
			route_len_pos:      C.uint8_t(config.Framer.RouteLenPos),
			route_order:        C.uint8_t(config.Framer.RouteOrder),
			payload_order:      C.uint8_t(config.Framer.PayloadOrder),
			options_order:      C.uint8_t(config.Framer.OptionsOrder),
			options_count_pos:  C.uint8_t(config.Framer.OptionsCountPos),
			options_key_first:  boolToCUint8(config.Framer.OptionsKeyFirst),
			header_length:      C.uint8_t(config.Framer.HeaderLength),
		},
		max_in_flight: C.uint32_t(config.MaxInFlight),
	}

	var ptr *C.MMAServer
	status := C.mma_server_create(&c, &ptr)
	if status != C.MMA_STATUS_OK {
		return nil, statusError(status)
	}
	if ptr == nil {
		return nil, errors.New("mma_server_create returned nil pointer")
	}

	s := &Server{ptr: ptr}
	runtime.SetFinalizer(s, (*Server).finalize)
	return s, nil
}

func boolToCUint8(value bool) C.uint8_t {
	if value {
		return 1
	}
	return 0
}

func (s *Server) finalize() {
	_ = s.Close()
}

// RegisterRoute registers a route with a Go handler.
func (s *Server) RegisterRoute(route string, handler RouteHandler) error {
	if s == nil {
		return ErrClosed
	}
	if handler == nil {
		return ErrInvalidArgument
	}
	if route == "" || len(route) > 255 {
		return ErrInvalidArgument
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ptr == nil {
		return ErrClosed
	}

	// Keep the Go callback alive while Rust keeps the route.
	handle := cgo.NewHandle(handler)

	routePtr := C.CBytes([]byte(route))
	defer C.free(routePtr)

	userData := C.mma_handle_to_ptr(C.uintptr_t(handle))
	status := C.mma_server_register_go_route(
		s.ptr,
		(*C.uint8_t)(routePtr),
		C.size_t(len(route)),
		userData,
	)

	if status != C.MMA_STATUS_OK {
		handle.Delete()
		return statusError(status)
	}

	s.handles = append(s.handles, handle)
	return nil
}

// Handle is a convenience alias for RegisterRoute.
func (s *Server) Handle(route string, handler RouteHandler) error {
	return s.RegisterRoute(route, handler)
}

func (s *Server) Start() error {
	if s == nil {
		return ErrClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ptr == nil {
		return ErrClosed
	}

	status := C.mma_server_start(s.ptr)
	if status != C.MMA_STATUS_OK {
		return statusError(status)
	}
	return nil
}

func (s *Server) BoundPort() (uint16, error) {
	if s == nil {
		return 0, ErrClosed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ptr == nil {
		return 0, ErrClosed
	}

	var port C.uint16_t
	status := C.mma_server_bound_port(s.ptr, &port)
	if status != C.MMA_STATUS_OK {
		return 0, statusError(status)
	}
	return uint16(port), nil
}

func (s *Server) Stop() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ptr == nil {
		return nil
	}

	status := C.mma_server_stop(s.ptr)
	if status != C.MMA_STATUS_OK {
		return statusError(status)
	}
	return nil
}

func (s *Server) Close() error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ptr == nil {
		return nil
	}

	C.mma_server_destroy(s.ptr)
	s.ptr = nil

	for _, handle := range s.handles {
		handle.Delete()
	}
	s.handles = nil

	runtime.SetFinalizer(s, nil)
	return nil
}

//export go_mma_route_callback
func go_mma_route_callback(
	request *C.MMARequest,
	response *C.MMARouteResponse,
	userData unsafe.Pointer,
) (status C.int32_t) {
	status = C.MMA_STATUS_CALLBACK_ERROR

	defer func() {
		if recover() != nil {
			status = C.MMA_STATUS_CALLBACK_ERROR
		}
	}()

	handleValue := C.mma_ptr_to_handle(userData)
	if handleValue == 0 {
		return status
	}

	handle := cgo.Handle(handleValue)
	value := handle.Value()
	handler, ok := value.(RouteHandler)
	if !ok || handler == nil {
		return status
	}

	goRequest, err := requestFromC(request)
	if err != nil {
		return status
	}

	builder := &ResponseBuilder{ptr: response}
	if err := handler(goRequest, builder); err != nil {
		return status
	}

	return C.MMA_STATUS_OK
}

func statusError(status C.int32_t) error {
	message := "unknown status"
	ptr := C.mma_status_message(status)
	if ptr != nil {
		message = C.GoString(ptr)
	}

	return &StatusError{
		Code:    int32(status),
		Message: message,
	}
}
