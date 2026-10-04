package cabi

type RequestOpcode uint8

const (
	OpcodeChannel RequestOpcode = 1
	OpcodeOnce    RequestOpcode = 2
)

type ResponseOpcode uint8

const (
	StatusOK            ResponseOpcode = 1
	StatusBadRequest    ResponseOpcode = 2
	StatusUnauthorized  ResponseOpcode = 3
	StatusForbidden     ResponseOpcode = 4
	StatusNotFound      ResponseOpcode = 5
	StatusConflict      ResponseOpcode = 6
	StatusInternalError ResponseOpcode = 7
	StatusMessage       ResponseOpcode = 8
)

type Option struct {
	Key   string
	Value string
}

type Request struct {
	Opcode  RequestOpcode
	Version uint8
	ReqID   uint32
	Route   string
	Options []Option
	Payload []byte
}

type Response struct {
	Opcode  ResponseOpcode
	Version uint8
	ReqID   uint32
	Options []Option
	Payload []byte
}

type FramerConfig struct {
	MaxMessageLength uint32
	OpcodePos        uint8
	VersionPos       uint8
	RouteLenPos      uint8
	RouteOrder       uint8
	PayloadOrder     uint8
	OptionsOrder     uint8
	OptionsCountPos  uint8
	OptionsKeyFirst  bool
	HeaderLength     uint8
}
