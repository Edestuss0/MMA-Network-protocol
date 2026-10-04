package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func startMockServer(t *testing.T, delay time.Duration) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				for {
					var header [HeaderLength]byte
					if _, err := io.ReadFull(c, header[:]); err != nil {
						return
					}
					tail := HeaderLength - ReservedSuffix
					optLen := binary.BigEndian.Uint32(header[tail : tail+4])
					payloadLen := binary.BigEndian.Uint32(header[tail+4 : tail+8])
					routeLen := uint64(header[RouteLenPos-1])
					bodyLen := uint64(optLen) + uint64(payloadLen) + routeLen
					body := make([]byte, bodyLen)
					if _, err := io.ReadFull(c, body); err != nil {
						return
					}
					if delay > 0 {
						time.Sleep(delay)
					}
					resp := header
					resp[OpcodePos-1] = ResponseOpcodeOK
					binary.BigEndian.PutUint32(resp[tail:tail+4], 0)
					binary.BigEndian.PutUint32(resp[tail+4:tail+8], 0)
					binary.BigEndian.PutUint32(resp[tail+8:tail+12], binary.BigEndian.Uint32(header[tail+8:tail+12]))
					if _, err := c.Write(resp[:]); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), func() { _ = ln.Close(); <-done }
}

func TestBuildRequestTemplate(t *testing.T) {
	frame, off, err := buildRequestTemplate("GET", nil, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if len(frame) != HeaderLength+len("hello")+len("GET") {
		t.Fatalf("bad frame len: %d", len(frame))
	}
	if frame[OpcodePos-1] != RequestOpcodeOnce || frame[VersionPos-1] != 1 {
		t.Fatal("bad opcode/version")
	}
	tail := HeaderLength - ReservedSuffix
	if got := binary.BigEndian.Uint32(frame[tail : tail+4]); got != 0 {
		t.Fatal("options len must be zero")
	}
	if got := binary.BigEndian.Uint32(frame[tail+4 : tail+8]); got != 5 {
		t.Fatal("payload len mismatch")
	}
	if off != tail+8 {
		t.Fatal("req id offset mismatch")
	}
	if string(frame[HeaderLength:HeaderLength+5]) != "hello" || string(frame[HeaderLength+5:]) != "GET" {
		t.Fatal("body ordering mismatch")
	}
	patchReqID(frame, off, 123456)
	if got := binary.BigEndian.Uint32(frame[off : off+4]); got != 123456 {
		t.Fatal("req id patch failed")
	}
}

func TestClosedLoopWorker(t *testing.T) {
	addr, stop := startMockServer(t, 100*time.Microsecond)
	defer stop()
	ready := make(chan struct{}, 1)
	start := make(chan struct{})
	go func() { time.Sleep(100 * time.Millisecond); close(start) }()
	res := closedLoopWorker(addr, 20, 3, "GET", []byte("payload"), 10, ready, start)
	<-ready
	if res.okCount != 20 || res.errorCount != 0 || res.histogram == nil || res.histogram.TotalCount() != 20 {
		t.Fatalf("bad closed-loop result: %+v", res)
	}
}

func TestOpenLoopWorker(t *testing.T) {
	addr, stop := startMockServer(t, 2*time.Millisecond)
	defer stop()
	ready := make(chan struct{}, 1)
	start := make(chan struct{})
	go func() { <-ready; close(start) }()
	res := openLoopWorker(addr, 30, 3, "GET", []byte("payload"), 100, 1000, ready, start)
	if res.okCount != 30 || res.errorCount != 0 || res.histogram == nil || res.histogram.TotalCount() != 30 {
		t.Fatalf("bad open-loop result: %+v", res)
	}
	if res.histogram.Mean() < 1000 {
		t.Fatalf("expected mock latency to be measurable, got %.1f us", res.histogram.Mean())
	}
}

func Example_parseArgs() {
	got, _ := parseArgs([]string{"127.0.0.1:8080", "--concurrency", "3", "--requests", "10", "--rate", "100"})
	fmt.Println(got.addr, got.concurrency, got.requests, *got.rate)
	// Output: 127.0.0.1:8080 3 10 100
}
