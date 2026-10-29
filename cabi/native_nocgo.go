//go:build !cgo

package cabi

// This file keeps the package type-checkable when cgo is disabled. It does
// not emulate MMA: every operation that needs the Rust C ABI returns
// ErrCGODisabled. Enable cgo to use the real implementation in native.go.

type Server struct{}
type ServerHandle struct{}
type ResponseBuilder struct{}

func DefaultServerConfig() (ServerConfig, error) { return ServerConfig{}, ErrCGODisabled }
func DefaultFramerConfig() (FramerConfig, error) { return FramerConfig{}, ErrCGODisabled }
func NewServer(ServerConfig) (*Server, error)    { return nil, ErrCGODisabled }

func (*Server) RegisterRoute(string, RouteHandler) error { return ErrCGODisabled }
func (s *Server) Handle(route string, handler RouteHandler) error {
	return s.RegisterRoute(route, handler)
}
func (*Server) RegisterChannelHandler(string, ChannelHandler) error { return ErrCGODisabled }
func (*Server) Start() error                                        { return ErrCGODisabled }
func (*Server) BoundPort() (uint16, error)                          { return 0, ErrCGODisabled }
func (*Server) GetHandle() (*ServerHandle, error)                   { return nil, ErrCGODisabled }
func (*Server) Stop() error                                         { return ErrCGODisabled }
func (*Server) Close() error                                        { return nil }

func (*ServerHandle) Close() error { return nil }
func (*ServerHandle) SendToChannel(uint32, Response) error {
	return ErrCGODisabled
}
func (*ServerHandle) ChangeConfig(ServerConfig) error { return ErrCGODisabled }

func (*ResponseBuilder) SetStatus(ResponseOpcode, ...uint8) error { return ErrCGODisabled }
func (b *ResponseBuilder) SetStatusBuilder(opcode ResponseOpcode, version ...uint8) error {
	return b.SetStatus(opcode, version...)
}
func (*ResponseBuilder) SetPayload([]byte) error        { return ErrCGODisabled }
func (*ResponseBuilder) AddOption(string, string) error { return ErrCGODisabled }
