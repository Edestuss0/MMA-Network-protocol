package cabi

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/ebitengine/purego"
)

// The following structs mirror include/mma.h's #[repr(C)] Rust types.
// Keep their field order and alignment in sync with the public C ABI.
type nativeBigBytesConfig struct {
	optionsLenOrder uint8
	payloadLenOrder uint8
	reqIDOrder      uint8
}

type nativeFramerConfig struct {
	maxMessageLength uint32
	opcodePos        uint8
	versionPos       uint8
	routeLenPos      uint8
	routeOrder       uint8
	payloadOrder     uint8
	optionsOrder     uint8
	optionsCountPos  uint8
	optionsKeyFirst  uint8
	headerLength     uint8
	bigBytesConfig   nativeBigBytesConfig
}

type nativeServerConfig struct {
	bindIP           unsafe.Pointer
	port             uint16
	maxBatch         uint32
	maxInFlight      uint32
	framer           nativeFramerConfig
	maxConnections   uint32
	drainTimeoutMS   uint64
	shutdownGraceMS  uint64
	requestTimeoutMS uint64
}

type nativeOption struct {
	key      unsafe.Pointer
	keyLen   uintptr
	value    unsafe.Pointer
	valueLen uintptr
}

type nativeRequest struct {
	route        unsafe.Pointer
	routeLen     uintptr
	opcode       uint8
	version      uint8
	requestID    uint32
	payload      unsafe.Pointer
	payloadLen   uintptr
	options      unsafe.Pointer
	optionsCount uintptr
}

type nativeResponse struct {
	inner unsafe.Pointer // Same pointer-sized ABI field as MMA_Response in mma.h.
}

type nativeAPI struct {
	library uintptr
	bridge  uintptr

	bridgeSetRouteCallback    func(uintptr) int32
	serverConfigDefault       func(unsafe.Pointer) int32
	serverCreate              func(unsafe.Pointer, unsafe.Pointer) int32
	serverLocalPort           func(unsafe.Pointer, unsafe.Pointer) int32
	serverRegisterRoute       func(unsafe.Pointer, unsafe.Pointer, uintptr, uintptr) int32
	serverRegisterChannel     func(unsafe.Pointer, unsafe.Pointer, uintptr, uintptr, uintptr) int32
	serverStart               func(unsafe.Pointer) int32
	serverStop                func(unsafe.Pointer) int32
	serverDestroy             func(unsafe.Pointer)
	serverGetHandle           func(unsafe.Pointer, unsafe.Pointer) int32
	serverHandleDestroy       func(unsafe.Pointer)
	serverHandleSendToChannel func(unsafe.Pointer, uint32, nativeResponse) int32
	serverHandleChangeConfig  func(unsafe.Pointer, unsafe.Pointer) int32
	responseNew               func(uint32) nativeResponse
	responseDestroy           func(nativeResponse)
	responseSetStatus         func(nativeResponse, uint8) nativeResponse
	responseSetPayload        func(nativeResponse, unsafe.Pointer, uintptr) nativeResponse
	responseAddOption         func(nativeResponse, unsafe.Pointer, uintptr, unsafe.Pointer, uintptr) nativeResponse

	routeCallback   uintptr
	channelCallback uintptr
}

var (
	apiMu     sync.Mutex
	loadedAPI *nativeAPI

	nextCallbackID atomic.Uint64
	callbackTable  sync.Map // uintptr -> RouteHandler or ChannelHandler
)

func getNativeAPI() (*nativeAPI, error) {
	apiMu.Lock()
	defer apiMu.Unlock()
	if loadedAPI != nil {
		return loadedAPI, nil
	}

	libraryPath, err := locateNativeLibrary()
	if err != nil {
		return nil, err
	}
	if runtime.GOOS == "windows" {
		addDLLSearchPath(filepath.Dir(libraryPath))
	}
	library, err := openNativeLibrary(libraryPath)
	if err != nil {
		return nil, fmt.Errorf("load MMA native library %q: %w", libraryPath, err)
	}

	bridgePath := filepath.Join(filepath.Dir(libraryPath), nativeBridgeLibraryName())
	bridge, err := openNativeLibrary(bridgePath)
	if err != nil {
		_ = closeNativeLibrary(library)
		return nil, fmt.Errorf("load MMA callback bridge %q: %w", bridgePath, err)
	}

	api := &nativeAPI{library: library, bridge: bridge}
	bindings := []struct {
		library uintptr
		name    string
		fn      any
	}{
		{bridge, "mma_go_bridge_set_route_callback", &api.bridgeSetRouteCallback},
		{bridge, "mma_go_server_register_route", &api.serverRegisterRoute},
		{library, "mma_server_config_default", &api.serverConfigDefault},
		{library, "mma_server_create", &api.serverCreate},
		{library, "mma_server_local_port", &api.serverLocalPort},
		{library, "mma_server_register_channel_handler", &api.serverRegisterChannel},
		{library, "mma_server_start", &api.serverStart},
		{library, "mma_server_stop", &api.serverStop},
		{library, "mma_server_destroy", &api.serverDestroy},
		{library, "mma_server_get_handle", &api.serverGetHandle},
		{library, "mma_server_handle_destroy", &api.serverHandleDestroy},
		{library, "mma_server_handle_send_to_channel", &api.serverHandleSendToChannel},
		{library, "mma_server_handle_change_config", &api.serverHandleChangeConfig},
		{library, "mma_response_new", &api.responseNew},
		{library, "mma_response_destroy", &api.responseDestroy},
		{library, "mma_response_set_status", &api.responseSetStatus},
		{library, "mma_response_set_payload", &api.responseSetPayload},
		{library, "mma_response_add_option", &api.responseAddOption},
	}
	for _, binding := range bindings {
		if err := bindNativeFunction(binding.library, binding.name, binding.fn); err != nil {
			_ = closeNativeLibrary(bridge)
			_ = closeNativeLibrary(library)
			return nil, err
		}
	}

	var callbackErr error
	api.routeCallback, callbackErr = createNativeCallback(nativeRouteCallback)
	if callbackErr != nil {
		_ = closeNativeLibrary(bridge)
		_ = closeNativeLibrary(library)
		return nil, fmt.Errorf("create MMA route callback: %w", callbackErr)
	}
	api.channelCallback, callbackErr = createNativeCallback(nativeChannelCallback)
	if callbackErr != nil {
		_ = closeNativeLibrary(bridge)
		_ = closeNativeLibrary(library)
		return nil, fmt.Errorf("create MMA channel callback: %w", callbackErr)
	}
	if err := statusError(api.bridgeSetRouteCallback(api.routeCallback)); err != nil {
		_ = closeNativeLibrary(bridge)
		_ = closeNativeLibrary(library)
		return nil, fmt.Errorf("register Go route callback with bridge: %w", err)
	}

	loadedAPI = api
	return api, nil
}

func bindNativeFunction(library uintptr, name string, fn any) (err error) {
	address, err := lookupNativeSymbol(library, name)
	if err != nil {
		return fmt.Errorf("resolve MMA symbol %q: %w", name, err)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("bind MMA symbol %q: %v", name, recovered)
		}
	}()
	purego.RegisterFunc(fn, address)
	return nil
}

func createNativeCallback(fn any) (callback uintptr, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("purego callback ABI is unsupported on %s/%s: %v", runtime.GOOS, runtime.GOARCH, recovered)
		}
	}()
	return purego.NewCallback(fn), nil
}

// DefaultServerConfig returns defaults supplied by the Rust C ABI.
func DefaultServerConfig() (ServerConfig, error) {
	api, err := getNativeAPI()
	if err != nil {
		return ServerConfig{}, err
	}
	var native nativeServerConfig
	if err := statusError(api.serverConfigDefault(unsafe.Pointer(&native))); err != nil {
		return ServerConfig{}, err
	}
	return serverConfigFromNative(native), nil
}

// DefaultFramerConfig returns the native default framer settings.
func DefaultFramerConfig() (FramerConfig, error) {
	config, err := DefaultServerConfig()
	if err != nil {
		return FramerConfig{}, err
	}
	return config.Framer, nil
}

func serverConfigFromNative(native nativeServerConfig) ServerConfig {
	return ServerConfig{
		BindIP:         cString(native.bindIP),
		Port:           native.port,
		MaxBatch:       native.maxBatch,
		MaxInFlight:    native.maxInFlight,
		Framer:         framerConfigFromNative(native.framer),
		MaxConnections: native.maxConnections,
		DrainTimeout:   time.Duration(native.drainTimeoutMS) * time.Millisecond,
		ShutdownGrace:  time.Duration(native.shutdownGraceMS) * time.Millisecond,
		RequestTimeout: time.Duration(native.requestTimeoutMS) * time.Millisecond,
	}
}

func framerConfigFromNative(native nativeFramerConfig) FramerConfig {
	return FramerConfig{
		MaxMessageLength: native.maxMessageLength,
		OpcodePos:        native.opcodePos,
		VersionPos:       native.versionPos,
		RouteLenPos:      native.routeLenPos,
		RouteOrder:       native.routeOrder,
		PayloadOrder:     native.payloadOrder,
		OptionsOrder:     native.optionsOrder,
		OptionsCountPos:  native.optionsCountPos,
		OptionsKeyFirst:  native.optionsKeyFirst != 0,
		HeaderLength:     native.headerLength,
		BigBytesConfig: BigBytesConfig{
			OptionsLenOrder: native.bigBytesConfig.optionsLenOrder,
			PayloadLenOrder: native.bigBytesConfig.payloadLenOrder,
			ReqIDOrder:      native.bigBytesConfig.reqIDOrder,
		},
	}
}

func framerConfigToNative(config FramerConfig) nativeFramerConfig {
	return nativeFramerConfig{
		maxMessageLength: config.MaxMessageLength,
		opcodePos:        config.OpcodePos,
		versionPos:       config.VersionPos,
		routeLenPos:      config.RouteLenPos,
		routeOrder:       config.RouteOrder,
		payloadOrder:     config.PayloadOrder,
		optionsOrder:     config.OptionsOrder,
		optionsCountPos:  config.OptionsCountPos,
		optionsKeyFirst:  boolToUint8(config.OptionsKeyFirst),
		headerLength:     config.HeaderLength,
		bigBytesConfig: nativeBigBytesConfig{
			optionsLenOrder: config.BigBytesConfig.OptionsLenOrder,
			payloadLenOrder: config.BigBytesConfig.PayloadLenOrder,
			reqIDOrder:      config.BigBytesConfig.ReqIDOrder,
		},
	}
}

func serverConfigToNative(config ServerConfig, bindIP []byte) (nativeServerConfig, error) {
	if config.BindIP == "" || strings.IndexByte(config.BindIP, 0) >= 0 {
		return nativeServerConfig{}, fmt.Errorf("%w: BindIP must be non-empty and cannot contain NUL", ErrInvalidArgument)
	}
	if config.DrainTimeout < 0 || config.ShutdownGrace < 0 || config.RequestTimeout < 0 {
		return nativeServerConfig{}, fmt.Errorf("%w: timeouts must not be negative", ErrInvalidConfig)
	}
	return nativeServerConfig{
		bindIP:           byteSlicePointer(bindIP),
		port:             config.Port,
		maxBatch:         config.MaxBatch,
		maxInFlight:      config.MaxInFlight,
		framer:           framerConfigToNative(normalizeFramerConfig(config.Framer)),
		maxConnections:   config.MaxConnections,
		drainTimeoutMS:   uint64(config.DrainTimeout / time.Millisecond),
		shutdownGraceMS:  uint64(config.ShutdownGrace / time.Millisecond),
		requestTimeoutMS: uint64(config.RequestTimeout / time.Millisecond),
	}, nil
}

func boolToUint8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func byteSlicePointer(data []byte) unsafe.Pointer {
	if len(data) == 0 {
		return nil
	}
	return unsafe.Pointer(&data[0])
}

func cString(pointer unsafe.Pointer) string {
	if pointer == nil {
		return ""
	}
	const maxCStringLength = 1 << 20
	for length := 0; length < maxCStringLength; length++ {
		if *(*byte)(unsafe.Add(pointer, length)) == 0 {
			return string(unsafe.Slice((*byte)(pointer), length))
		}
	}
	return ""
}

// Server owns an MMA_Server allocated by the Rust shared library.
type Server struct {
	mu          sync.Mutex
	lifecycleMu sync.Mutex
	api         *nativeAPI
	ptr         unsafe.Pointer
	handles     []uintptr
	stopping    bool
}

func NewServer(config ServerConfig) (*Server, error) {
	api, err := getNativeAPI()
	if err != nil {
		return nil, err
	}
	bindIP := append([]byte(config.BindIP), 0)
	nativeConfig, err := serverConfigToNative(config, bindIP)
	if err != nil {
		return nil, err
	}
	var pointer unsafe.Pointer
	status := api.serverCreate(
		unsafe.Pointer(&nativeConfig),
		unsafe.Pointer(&pointer),
	)
	runtime.KeepAlive(bindIP)
	runtime.KeepAlive(&nativeConfig)
	if err := statusError(status); err != nil {
		return nil, err
	}
	if pointer == nil {
		return nil, errors.New("mma_server_create returned a nil pointer")
	}
	server := &Server{api: api, ptr: pointer}
	runtime.SetFinalizer(server, (*Server).finalize)
	return server, nil
}

func (s *Server) finalize() { _ = s.Close() }

// RegisterRoute registers a one-shot route. Register routes before Start.
func (s *Server) RegisterRoute(route string, handler RouteHandler) error {
	if s == nil {
		return ErrClosed
	}
	if handler == nil || len(route) == 0 || len(route) > 255 || !utf8.ValidString(route) || strings.IndexByte(route, 0) >= 0 {
		return ErrInvalidArgument
	}
	routeBytes := []byte(route)
	id := keepCallback(handler)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == nil {
		releaseCallback(id)
		return ErrClosed
	}
	if s.stopping {
		releaseCallback(id)
		return ErrServer
	}
	status := s.api.serverRegisterRoute(s.ptr, byteSlicePointer(routeBytes), uintptr(len(routeBytes)), id)
	runtime.KeepAlive(routeBytes)
	if err := statusError(status); err != nil {
		releaseCallback(id)
		return err
	}
	s.handles = append(s.handles, id)
	return nil
}

// Handle is an alias for RegisterRoute.
func (s *Server) Handle(route string, handler RouteHandler) error {
	return s.RegisterRoute(route, handler)
}

// RegisterChannelHandler registers a handler for messages delivered to a channel route.
func (s *Server) RegisterChannelHandler(route string, handler ChannelHandler) error {
	if s == nil {
		return ErrClosed
	}
	if handler == nil || len(route) == 0 || len(route) > 255 || !utf8.ValidString(route) || strings.IndexByte(route, 0) >= 0 {
		return ErrInvalidArgument
	}
	routeBytes := []byte(route)
	id := keepCallback(handler)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ptr == nil {
		releaseCallback(id)
		return ErrClosed
	}
	if s.stopping {
		releaseCallback(id)
		return ErrServer
	}
	status := s.api.serverRegisterChannel(s.ptr, byteSlicePointer(routeBytes), uintptr(len(routeBytes)), s.api.channelCallback, id)
	runtime.KeepAlive(routeBytes)
	if err := statusError(status); err != nil {
		releaseCallback(id)
		return err
	}
	s.handles = append(s.handles, id)
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
	return statusError(s.api.serverStart(s.ptr))
}

// BoundPort returns the listener port reported by the native server.
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
	var port uint16
	if err := statusError(s.api.serverLocalPort(s.ptr, unsafe.Pointer(&port))); err != nil {
		return 0, err
	}
	return port, nil
}

// GetHandle returns a separately owned native handle for channel sends and config updates.
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
	var pointer unsafe.Pointer
	if err := statusError(s.api.serverGetHandle(s.ptr, unsafe.Pointer(&pointer))); err != nil {
		return nil, err
	}
	if pointer == nil {
		return nil, errors.New("mma_server_get_handle returned a nil pointer")
	}
	handle := &ServerHandle{api: s.api, ptr: pointer}
	runtime.SetFinalizer(handle, (*ServerHandle).finalize)
	return handle, nil
}

// Stop stops the native server but keeps the wrapper allocated until Close.
func (s *Server) Stop() error {
	if s == nil {
		return nil
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	s.mu.Lock()
	pointer := s.ptr
	if pointer == nil {
		s.mu.Unlock()
		return nil
	}
	s.stopping = true
	s.mu.Unlock()

	err := statusError(s.api.serverStop(pointer))
	s.mu.Lock()
	s.stopping = false
	s.mu.Unlock()
	return err
}

// Close destroys the native server and then releases its callback registrations.
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()

	s.mu.Lock()
	pointer := s.ptr
	s.ptr = nil
	s.stopping = true
	handles := s.handles
	s.handles = nil
	s.mu.Unlock()
	runtime.SetFinalizer(s, nil)
	if pointer == nil {
		return nil
	}
	// The native destroy joins its server thread before callback values are discarded.
	s.api.serverDestroy(pointer)
	for _, id := range handles {
		releaseCallback(id)
	}
	return nil
}

// ServerHandle is independently owned by the native server runtime.
type ServerHandle struct {
	mu  sync.Mutex
	api *nativeAPI
	ptr unsafe.Pointer
}

func (h *ServerHandle) finalize() { _ = h.Close() }

func (h *ServerHandle) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	pointer := h.ptr
	h.ptr = nil
	h.mu.Unlock()
	runtime.SetFinalizer(h, nil)
	if pointer != nil {
		h.api.serverHandleDestroy(pointer)
	}
	return nil
}

// SendToChannel sends a message to an established channel connection.
func (h *ServerHandle) SendToChannel(connectionID uint32, message Response) error {
	if h == nil {
		return ErrClosed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ptr == nil {
		return ErrClosed
	}
	response := h.api.responseNew(message.ReqID)
	if response.inner == nil {
		return errors.New("mma_response_new returned a nil response")
	}
	defer h.api.responseDestroy(response)

	opcode := message.Opcode
	if opcode == 0 {
		opcode = StatusMessage
	}
	response = h.api.responseSetStatus(response, uint8(opcode))
	if response.inner == nil {
		return fmt.Errorf("%w: unsupported response opcode %d", ErrInvalidArgument, opcode)
	}
	var err error
	response, err = setNativeResponsePayload(h.api, response, message.Payload)
	if err != nil {
		return err
	}
	for _, option := range message.Options {
		response, err = addNativeResponseOption(h.api, response, option.Key, option.Value)
		if err != nil {
			return err
		}
	}
	return statusError(h.api.serverHandleSendToChannel(h.ptr, connectionID, response))
}

// ChangeConfig applies an updated configuration to the running server.
func (h *ServerHandle) ChangeConfig(config ServerConfig) error {
	if h == nil {
		return ErrClosed
	}
	bindIP := append([]byte(config.BindIP), 0)
	nativeConfig, err := serverConfigToNative(config, bindIP)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ptr == nil {
		return ErrClosed
	}
	err = statusError(h.api.serverHandleChangeConfig(h.ptr, unsafe.Pointer(&nativeConfig)))
	runtime.KeepAlive(bindIP)
	runtime.KeepAlive(&nativeConfig)
	return err
}

// ResponseBuilder updates the response object owned by the native library.
// It is borrowed and valid only during RouteHandler execution.
type ResponseBuilder struct {
	api      *nativeAPI
	response nativeResponse
}

// SetStatus accepts a legacy optional version argument; the current C ABI sets protocol version itself.
func (b *ResponseBuilder) SetStatus(opcode ResponseOpcode, _ ...uint8) error {
	if b == nil || b.api == nil || b.response.inner == nil {
		return ErrInvalidArgument
	}
	updated := b.api.responseSetStatus(b.response, uint8(opcode))
	if updated.inner == nil {
		return fmt.Errorf("%w: unsupported response opcode %d", ErrInvalidArgument, opcode)
	}
	b.response = updated
	return nil
}

func (b *ResponseBuilder) SetStatusBuilder(opcode ResponseOpcode, version ...uint8) error {
	return b.SetStatus(opcode, version...)
}

func (b *ResponseBuilder) SetPayload(payload []byte) error {
	if b == nil || b.api == nil || b.response.inner == nil {
		return ErrInvalidArgument
	}
	updated, err := setNativeResponsePayload(b.api, b.response, payload)
	if err != nil {
		return err
	}
	b.response = updated
	return nil
}

func (b *ResponseBuilder) AddOption(key, value string) error {
	if b == nil || b.api == nil || b.response.inner == nil {
		return ErrInvalidArgument
	}
	updated, err := addNativeResponseOption(b.api, b.response, key, value)
	if err != nil {
		return err
	}
	b.response = updated
	return nil
}

func setNativeResponsePayload(api *nativeAPI, response nativeResponse, payload []byte) (nativeResponse, error) {
	updated := api.responseSetPayload(response, byteSlicePointer(payload), uintptr(len(payload)))
	runtime.KeepAlive(payload)
	if updated.inner == nil {
		return response, ErrInvalidArgument
	}
	return updated, nil
}

func addNativeResponseOption(api *nativeAPI, response nativeResponse, key, value string) (nativeResponse, error) {
	if key == "" || value == "" || strings.IndexByte(key, 0) >= 0 || strings.IndexByte(value, 0) >= 0 {
		return response, fmt.Errorf("%w: option key and value must not be empty or contain NUL", ErrInvalidArgument)
	}
	keyBytes, valueBytes := []byte(key), []byte(value)
	updated := api.responseAddOption(response,
		byteSlicePointer(keyBytes), uintptr(len(keyBytes)),
		byteSlicePointer(valueBytes), uintptr(len(valueBytes)))
	runtime.KeepAlive(keyBytes)
	runtime.KeepAlive(valueBytes)
	if updated.inner == nil {
		return response, ErrInvalidArgument
	}
	return updated, nil
}

func requestFromNative(native nativeRequest) (*Request, error) {
	route, err := copyNativeBytes(native.route, native.routeLen)
	if err != nil {
		return nil, err
	}
	payload, err := copyNativeBytes(native.payload, native.payloadLen)
	if err != nil {
		return nil, err
	}
	count, err := nativeSizeToInt(native.optionsCount)
	if err != nil || count > 255 {
		return nil, ErrInvalidArgument
	}
	if count > 0 && native.options == nil {
		return nil, ErrInvalidArgument
	}
	options := make([]Option, 0, count)
	if count > 0 {
		nativeOptions := unsafe.Slice((*nativeOption)(native.options), count)
		for _, option := range nativeOptions {
			key, err := copyNativeBytes(option.key, option.keyLen)
			if err != nil {
				return nil, err
			}
			value, err := copyNativeBytes(option.value, option.valueLen)
			if err != nil {
				return nil, err
			}
			options = append(options, Option{Key: string(key), Value: string(value)})
		}
	}
	return &Request{
		Opcode:  RequestOpcode(native.opcode),
		Version: native.version,
		ReqID:   native.requestID,
		Route:   string(route),
		Options: options,
		Payload: payload,
	}, nil
}

func copyNativeBytes(pointer unsafe.Pointer, size uintptr) ([]byte, error) {
	n, err := nativeSizeToInt(size)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	if pointer == nil {
		return nil, ErrInvalidArgument
	}
	result := make([]byte, n)
	copy(result, unsafe.Slice((*byte)(pointer), n))
	return result, nil
}

func nativeSizeToInt(size uintptr) (int, error) {
	maxInt := uintptr(^uint(0) >> 1)
	if size > maxInt {
		return 0, errors.New("native size_t value does not fit into Go int")
	}
	return int(size), nil
}

func keepCallback(callback any) uintptr {
	id := uintptr(nextCallbackID.Add(1))
	if id == 0 {
		id = uintptr(nextCallbackID.Add(1))
	}
	callbackTable.Store(id, callback)
	return id
}

func releaseCallback(id uintptr) { callbackTable.Delete(id) }

func callbackByID(id uintptr) (any, bool) { return callbackTable.Load(id) }

func nativeRouteCallback(_ purego.CDecl, requestPointer unsafe.Pointer, responsePointer unsafe.Pointer, userData uintptr) (result int32) {
	if responsePointer == nil {
		return nativeStatusInvalidArgument
	}
	response := (*nativeResponse)(responsePointer)
	result = nativeStatusOK
	defer func() {
		if recover() != nil {
			if api, err := getNativeAPI(); err == nil {
				*response = setCallbackError(api, *response, StatusInternalError, "Internal server error")
				result = nativeStatusOK
			} else {
				result = nativeStatusServerError
			}
		}
	}()

	api, err := getNativeAPI()
	if err != nil {
		return nativeStatusServerError
	}
	if requestPointer == nil {
		*response = setCallbackError(api, *response, StatusBadRequest, "Invalid request")
		return nativeStatusOK
	}
	value, ok := callbackByID(userData)
	handler, isRouteHandler := value.(RouteHandler)
	if !ok || !isRouteHandler || handler == nil {
		*response = setCallbackError(api, *response, StatusInternalError, "Internal server error")
		return nativeStatusOK
	}
	goRequest, err := requestFromNative(*(*nativeRequest)(requestPointer))
	if err != nil {
		*response = setCallbackError(api, *response, StatusBadRequest, "Invalid request")
		return nativeStatusOK
	}
	builder := &ResponseBuilder{api: api, response: *response}
	if err := handler(goRequest, builder); err != nil {
		builder.response = setCallbackError(api, builder.response, StatusInternalError, "Internal server error")
	}
	*response = builder.response
	return nativeStatusOK
}

func nativeChannelCallback(_ purego.CDecl, requestPointer unsafe.Pointer, connectionID uint32, userData uintptr) (result int32) {
	result = nativeStatusServerError
	defer func() {
		if recover() != nil {
			result = nativeStatusServerError
		}
	}()
	if requestPointer == nil {
		return nativeStatusInvalidArgument
	}
	value, ok := callbackByID(userData)
	handler, isChannelHandler := value.(ChannelHandler)
	if !ok || !isChannelHandler || handler == nil {
		return nativeStatusInvalidArgument
	}
	request, err := requestFromNative(*(*nativeRequest)(requestPointer))
	if err != nil {
		return nativeStatusInvalidArgument
	}
	if err := handler(request, connectionID); err != nil {
		return nativeStatusServerError
	}
	return nativeStatusOK
}

const (
	nativeStatusOK              int32 = 0
	nativeStatusInvalidArgument int32 = -1
	nativeStatusServerError     int32 = -3
)

func setCallbackError(api *nativeAPI, response nativeResponse, opcode ResponseOpcode, message string) (result nativeResponse) {
	result = response
	defer func() { _ = recover() }()
	if result.inner == nil {
		return result
	}
	result = api.responseSetStatus(result, uint8(opcode))
	if result.inner == nil {
		return result
	}
	payload := []byte(message)
	updated := api.responseSetPayload(result, byteSlicePointer(payload), uintptr(len(payload)))
	runtime.KeepAlive(payload)
	if updated.inner != nil {
		result = updated
	}
	return result
}

// normalizeFramerConfig supplies the C ABI's default nested ordering when omitted.
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
