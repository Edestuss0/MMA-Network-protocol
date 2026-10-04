// mma-loadtest is a load-testing client for the MMA protocol.
//
// The frame is built manually according to the current FramerConfig layout.
// If the server-side FramerConfig changes, update only the protocol constants
// in the section below.
//
// Modes:
//   - closed-loop (default): send one request, wait for its response, repeat.
//   - open-loop (--rate N): schedule N requests/sec per connection without
//     waiting for responses; latency is measured from the individual send
//     timestamp matched by req_id.
//
// Examples:
//   go run . 127.0.0.1:8080 --concurrency 50 --requests 20000
//   go run . 127.0.0.1:8080 --concurrency 50 --requests 20000 --rate 500
//
// Options:
//   --concurrency N   number of parallel TCP connections (default 50)
//   --requests N      measured requests per connection (default 2000)
//   --warmup N        warmup requests per connection, excluded from stats (default 100)
//   --route STR       route (default "GET")
//   --payload STR     request payload (default "hello world, this is a small payload")
//   --rate N          open-loop target requests/sec PER CONNECTION

package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// ============================================================================
// Frame layout — current FramerConfig.
// Positions are 1-indexed, exactly like the Rust implementation.
// ============================================================================
const (
	HeaderLength      = 29
	OpcodePos         = 1
	VersionPos        = 4
	RouteLenPos       = 6
	OptionsCountPos   = 7
	ReservedSuffix    = 12 // [options_len u32 BE][payload_len u32 BE][req_id u32 BE]
	PayloadOrder      = 1
	OptionsOrder      = 2
	RouteOrder        = 3
	OptionsKeyFirst   = true
	RequestOpcodeOnce = 2
	ResponseOpcodeOK  = 1
	WarmupMarker      = uint32(0x80000000)
	MaxHistogramUs    = int64(60_000_000)
)

func posToIndex(pos int) int { return pos - 1 }

func encodeOption(key, value string, out *[]byte) {
	k := []byte(key)
	v := []byte(value)

	var lenBuf [4]byte
	binary.BigEndian.PutUint16(lenBuf[0:2], uint16(len(k)))
	binary.BigEndian.PutUint16(lenBuf[2:4], uint16(len(v)))
	*out = append(*out, lenBuf[:]...)

	if OptionsKeyFirst {
		*out = append(*out, k...)
		*out = append(*out, v...)
	} else {
		*out = append(*out, v...)
		*out = append(*out, k...)
	}
}

// buildRequestTemplate builds a frame once. The req_id field is left at zero
// and patched in-place before every write.
func buildRequestTemplate(route string, options [][2]string, payload []byte) ([]byte, int, error) {
	routeBytes := []byte(route)
	if len(routeBytes) > 255 {
		return nil, 0, fmt.Errorf("route is too long: %d bytes, max 255", len(routeBytes))
	}
	if len(options) > 255 {
		return nil, 0, fmt.Errorf("too many options: %d, max 255", len(options))
	}

	optionsBytes := make([]byte, 0)
	for _, option := range options {
		if len([]byte(option[0])) > 65535 || len([]byte(option[1])) > 65535 {
			return nil, 0, fmt.Errorf("option key/value exceeds u16 length limit")
		}
		encodeOption(option[0], option[1], &optionsBytes)
	}
	if uint64(len(optionsBytes)) > uint64(^uint32(0)) {
		return nil, 0, errors.New("encoded options exceed u32 length limit")
	}
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return nil, 0, errors.New("payload exceeds u32 length limit")
	}

	header := make([]byte, HeaderLength)
	header[posToIndex(OpcodePos)] = RequestOpcodeOnce
	header[posToIndex(VersionPos)] = 1
	header[posToIndex(RouteLenPos)] = byte(len(routeBytes))
	header[posToIndex(OptionsCountPos)] = byte(len(options))

	tailStart := HeaderLength - ReservedSuffix
	binary.BigEndian.PutUint32(header[tailStart:tailStart+4], uint32(len(optionsBytes)))
	binary.BigEndian.PutUint32(header[tailStart+4:tailStart+8], uint32(len(payload)))
	// req_id: tailStart+8 : tailStart+12 remains zero.

	frame := make([]byte, 0, HeaderLength+len(optionsBytes)+len(routeBytes)+len(payload))
	frame = append(frame, header...)
	for i := uint8(1); i <= 3; i++ {
		switch i {
		case OptionsOrder:
			frame = append(frame, optionsBytes...)
		case RouteOrder:
			frame = append(frame, routeBytes...)
		case PayloadOrder:
			frame = append(frame, payload...)
		}
	}

	return frame, tailStart + 8, nil
}

func patchReqID(template []byte, reqIDOffset int, reqID uint32) {
	binary.BigEndian.PutUint32(template[reqIDOffset:reqIDOffset+4], reqID)
}

type parsedResponse struct {
	opcode uint8
	reqID  uint32
}

// readResponseFrame reuses header/body buffers and therefore does not allocate
// on every response after the first body-capacity growth.
func readResponseFrame(
	r io.Reader,
	headerBuf *[HeaderLength]byte,
	bodyBuf *[]byte,
) (parsedResponse, error) {
	if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
		return parsedResponse{}, err
	}

	tailStart := HeaderLength - ReservedSuffix
	optionsLen := binary.BigEndian.Uint32(headerBuf[tailStart : tailStart+4])
	payloadLen := binary.BigEndian.Uint32(headerBuf[tailStart+4 : tailStart+8])
	reqID := binary.BigEndian.Uint32(headerBuf[tailStart+8 : tailStart+12])
	opcode := headerBuf[posToIndex(OpcodePos)]

	bodyLen64 := uint64(optionsLen) + uint64(payloadLen)
	maxInt := uint64(^uint(0) >> 1)
	if bodyLen64 > maxInt {
		return parsedResponse{}, fmt.Errorf("response body too large: %d bytes", bodyLen64)
	}
	bodyLen := int(bodyLen64)

	if cap(*bodyBuf) < bodyLen {
		*bodyBuf = make([]byte, bodyLen)
	} else {
		*bodyBuf = (*bodyBuf)[:bodyLen]
	}
	if bodyLen > 0 {
		if _, err := io.ReadFull(r, *bodyBuf); err != nil {
			return parsedResponse{}, err
		}
	}

	return parsedResponse{opcode: opcode, reqID: reqID}, nil
}

func writeAll(w io.Writer, buf []byte) error {
	for len(buf) > 0 {
		n, err := w.Write(buf)
		if n > 0 {
			buf = buf[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func newHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(1, MaxHistogramUs, 3)
}

type workerStats struct {
	histogram      *hdrhistogram.Histogram
	okCount        uint64
	errorCount     uint64
	latencyDropped int64
}

type args struct {
	addr        string
	concurrency uint64
	requests    uint64
	warmup      uint64
	route       string
	payload     string
	rate        *float64
}

func parseArgs(argv []string) (args, error) {
	if len(argv) < 1 || strings.HasPrefix(argv[0], "--") || argv[0] == "" {
		return args{}, errors.New("usage: mma-loadtest <host:port> [--concurrency N] [--requests N] [--warmup N] [--route STR] [--payload STR] [--rate N]")
	}

	result := args{
		addr:        argv[0],
		concurrency: 50,
		requests:    2000,
		warmup:      100,
		route:       "GET",
		payload:     "hello world, this is a small payload",
	}

	fs := flag.NewFlagSet("mma-loadtest", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	concurrency := fs.Uint64("concurrency", result.concurrency, "parallel TCP connections")
	requests := fs.Uint64("requests", result.requests, "measured requests per connection")
	warmup := fs.Uint64("warmup", result.warmup, "warmup requests per connection")
	route := fs.String("route", result.route, "request route")
	payload := fs.String("payload", result.payload, "request payload")
	rate := fs.Float64("rate", 0, "open-loop requests/sec per connection; <=0 disables open-loop")

	if err := fs.Parse(argv[1:]); err != nil {
		return args{}, err
	}
	if fs.NArg() != 0 {
		return args{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if *concurrency == 0 {
		return args{}, errors.New("--concurrency must be > 0")
	}
	if *requests == 0 {
		return args{}, errors.New("--requests must be > 0")
	}
	if *route == "" {
		return args{}, errors.New("--route must not be empty")
	}
	if *rate < 0 {
		return args{}, errors.New("--rate must be >= 0")
	}

	result.concurrency = *concurrency
	result.requests = *requests
	result.warmup = *warmup
	result.route = *route
	result.payload = *payload
	if *rate > 0 {
		result.rate = rate
	}
	return result, nil
}

func dial(addr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		if err := tcpConn.SetNoDelay(true); err != nil {
			fmt.Fprintf(os.Stderr, "set_nodelay failed for %s: %v\n", addr, err)
		}
	}
	return conn, nil
}

// ---------------------------------------------------------------------------
// Closed loop.
// ---------------------------------------------------------------------------
func closedLoopWorker(
	addr string,
	requests, warmup uint64,
	route string,
	payload []byte,
	reqIDBase uint32,
	ready chan<- struct{},
	start <-chan struct{},
) workerStats {
	conn, err := dial(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect error to %s: %v\n", addr, err)
		ready <- struct{}{}
		<-start
		return workerStats{histogram: nil, errorCount: requests}
	}
	defer conn.Close()

	template, reqIDOffset, err := buildRequestTemplate(route, nil, payload)
	if err != nil {
		ready <- struct{}{}
		<-start
		fmt.Fprintf(os.Stderr, "build request template error: %v\n", err)
		return workerStats{histogram: nil, errorCount: requests}
	}
	sendBuf := append([]byte(nil), template...)
	var headerBuf [HeaderLength]byte
	bodyBuf := make([]byte, 0)

	for i := uint64(0); i < warmup; i++ {
		patchReqID(sendBuf, reqIDOffset, WarmupMarker|uint32(i))
		if err := writeAll(conn, sendBuf); err != nil {
			break
		}
		_, _ = readResponseFrame(conn, &headerBuf, &bodyBuf)
	}

	ready <- struct{}{}
	<-start

	hist := newHistogram()
	var okCount, errorCount uint64
	var latencyDropped int64

	for i := uint64(0); i < requests; i++ {
		reqID := reqIDBase + uint32(i)
		patchReqID(sendBuf, reqIDOffset, reqID)

		startAt := time.Now()
		if err := writeAll(conn, sendBuf); err != nil {
			errorCount++
			fmt.Fprintf(os.Stderr, "io error on connection: %v\n", err)
			break
		}
		resp, err := readResponseFrame(conn, &headerBuf, &bodyBuf)
		elapsedUs := time.Since(startAt).Microseconds()
		if elapsedUs < 1 {
			elapsedUs = 1
		}

		if err != nil {
			errorCount++
			fmt.Fprintf(os.Stderr, "io error on connection: %v\n", err)
			break
		}
		if resp.opcode == ResponseOpcodeOK {
			okCount++
			if err := hist.RecordValue(elapsedUs); err != nil {
				latencyDropped++
			}
		} else {
			errorCount++
		}
	}

	return workerStats{
		histogram:      hist,
		okCount:        okCount,
		errorCount:     errorCount,
		latencyDropped: latencyDropped,
	}
}

// ---------------------------------------------------------------------------
// Open loop.
// ---------------------------------------------------------------------------
func openLoopWorker(
	addr string,
	requests, warmup uint64,
	route string,
	payload []byte,
	reqIDBase uint32,
	ratePerSec float64,
	ready chan<- struct{},
	start <-chan struct{},
) workerStats {
	conn, err := dial(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect error to %s: %v\n", addr, err)
		ready <- struct{}{}
		<-start
		return workerStats{histogram: nil, errorCount: requests}
	}
	defer conn.Close()

	template, reqIDOffset, err := buildRequestTemplate(route, nil, payload)
	if err != nil {
		ready <- struct{}{}
		<-start
		fmt.Fprintf(os.Stderr, "build request template error: %v\n", err)
		return workerStats{histogram: nil, errorCount: requests}
	}

	inFlight := make(map[uint32]time.Time, minInt(requests, 1_000_000))
	var inFlightMu sync.Mutex
	var okCount uint64
	var errorCount uint64
	var latencyDropped int64

	readerHist := newHistogram()
	readerDone := make(chan struct{})

	go func() {
		defer close(readerDone)
		var headerBuf [HeaderLength]byte
		bodyBuf := make([]byte, 0)

		for {
			resp, err := readResponseFrame(conn, &headerBuf, &bodyBuf)
			if err != nil {
				return
			}
			if resp.reqID&WarmupMarker != 0 {
				continue
			}

			inFlightMu.Lock()
			sentAt, found := inFlight[resp.reqID]
			if found {
				delete(inFlight, resp.reqID)
			}
			inFlightMu.Unlock()

			if !found {
				// Unknown/non-measured req_id: same behavior as the Rust client —
				// ignore it rather than turning it into a protocol error.
				continue
			}

			if resp.opcode != ResponseOpcodeOK {
				atomic.AddUint64(&errorCount, 1)
				continue
			}

			atomic.AddUint64(&okCount, 1)
			elapsedUs := time.Since(sentAt).Microseconds()
			if elapsedUs < 1 {
				elapsedUs = 1
			}
			if err := readerHist.RecordValue(elapsedUs); err != nil {
				atomic.AddInt64(&latencyDropped, 1)
			}
		}
	}()

	// Warmup is intentionally sent without timing or in-flight bookkeeping.
	warmupBuf := append([]byte(nil), template...)
	for i := uint64(0); i < warmup; i++ {
		patchReqID(warmupBuf, reqIDOffset, WarmupMarker|uint32(i))
		if err := writeAll(conn, warmupBuf); err != nil {
			break
		}
	}
	// Preserve the original 50ms warmup grace period.
	time.Sleep(50 * time.Millisecond)

	ready <- struct{}{}
	<-start

	interval := time.Duration(float64(time.Second) / ratePerSec)
	if interval < time.Nanosecond {
		interval = time.Nanosecond
	}
	scheduleStart := time.Now()
	sendBuf := append([]byte(nil), template...)

	for i := uint64(0); i < requests; i++ {
		target := scheduleStart.Add(time.Duration(i) * interval)
		if sleepFor := time.Until(target); sleepFor > 0 {
			time.Sleep(sleepFor)
		}

		reqID := reqIDBase + uint32(i)
		sentAt := time.Now()
		inFlightMu.Lock()
		inFlight[reqID] = sentAt
		inFlightMu.Unlock()

		patchReqID(sendBuf, reqIDOffset, reqID)
		if err := writeAll(conn, sendBuf); err != nil {
			// Remove the failed request from in-flight so a later drain cannot
			// count it a second time.
			inFlightMu.Lock()
			delete(inFlight, reqID)
			inFlightMu.Unlock()
			atomic.AddUint64(&errorCount, 1)
			fmt.Fprintf(os.Stderr, "io error on connection: %v\n", err)
			break
		}
	}

	// Drain outstanding replies for up to five seconds, matching the Rust
	// implementation's grace period.
	drainDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(drainDeadline) {
		inFlightMu.Lock()
		remaining := len(inFlight)
		inFlightMu.Unlock()
		if remaining == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	inFlightMu.Lock()
	timedOut := uint64(len(inFlight))
	inFlightMu.Unlock()
	if timedOut > 0 {
		atomic.AddUint64(&errorCount, timedOut)
		fmt.Fprintf(os.Stderr, "%d requests did not receive a response within the 5s drain grace period (--rate %.3f may be too aggressive for the server)\n", timedOut, ratePerSec)
	}

	// Closing the connection unblocks a reader goroutine stuck in ReadFull.
	_ = conn.Close()
	<-readerDone

	return workerStats{
		histogram:      readerHist,
		okCount:        atomic.LoadUint64(&okCount),
		errorCount:     atomic.LoadUint64(&errorCount),
		latencyDropped: atomic.LoadInt64(&latencyDropped),
	}
}

func minInt(v uint64, limit int) int {
	if v > uint64(limit) {
		return limit
	}
	return int(v)
}

func reqIDBaseForWorker(workerIdx uint64) uint32 {
	// Keep the exact ID layout of the Rust client for compatibility with its
	// request-id assumptions. The high bit is reserved for warmup frames.
	return uint32(workerIdx*1_000_000) &^ WarmupMarker
}

func main() {
	args, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	mode := "closed-loop"
	if args.rate != nil {
		mode = fmt.Sprintf("open-loop @ %.3f req/s/conn", *args.rate)
	}
	fmt.Printf(
		"MMA load test: addr=%s concurrency=%d requests/conn=%d warmup/conn=%d route=%s payload_len=%d mode=%s\n",
		args.addr,
		args.concurrency,
		args.requests,
		args.warmup,
		args.route,
		len([]byte(args.payload)),
		mode,
	)

	// ready is the equivalent of the Rust Barrier's readiness phase. Every
	// worker signals after connect + warmup. Then the main goroutine records the
	// measurement start timestamp and releases all workers at once.
	ready := make(chan struct{}, args.concurrency)
	start := make(chan struct{})
	results := make(chan workerStats, args.concurrency)
	var wg sync.WaitGroup
	wg.Add(int(args.concurrency))

	for workerIdx := uint64(0); workerIdx < args.concurrency; workerIdx++ {
		workerIdx := workerIdx
		payload := []byte(args.payload)
		reqIDBase := reqIDBaseForWorker(workerIdx)

		go func() {
			defer wg.Done()
			if args.rate != nil {
				results <- openLoopWorker(
					args.addr,
					args.requests,
					args.warmup,
					args.route,
					payload,
					reqIDBase,
					*args.rate,
					ready,
					start,
				)
				return
			}
			results <- closedLoopWorker(
				args.addr,
				args.requests,
				args.warmup,
				args.route,
				payload,
				reqIDBase,
				ready,
				start,
			)
		}()
	}

	for i := uint64(0); i < args.concurrency; i++ {
		<-ready
	}
	overallStart := time.Now()
	close(start)

	wg.Wait()
	close(results)
	elapsed := time.Since(overallStart)

	merged := newHistogram()
	var totalOK, totalErr, latencyDropped uint64
	for result := range results {
		if result.histogram != nil {
			latencyDropped += uint64(result.latencyDropped)
			merged.Merge(result.histogram)
		}
		totalOK += result.okCount
		totalErr += result.errorCount
	}

	fmt.Println("\n=== Результаты ===")
	fmt.Printf("Всего успешных запросов: %d\n", totalOK)
	fmt.Printf("Всего ошибок:            %d\n", totalErr)
	fmt.Printf("Время измеряемой фазы:   %s\n", elapsed)
	if elapsed > 0 {
		fmt.Printf("Throughput:              %.0f req/sec\n", float64(totalOK)/elapsed.Seconds())
	} else {
		fmt.Println("Throughput:              0 req/sec")
	}

	if latencyDropped > 0 {
		fmt.Printf("Latency > %d us (не попали в HDR): %d\n", MaxHistogramUs, latencyDropped)
	}

	if merged.TotalCount() > 0 {
		fmt.Printf("Latency min:   %d us\n", merged.Min())
		fmt.Printf("Latency p50:   %d us\n", merged.ValueAtQuantile(50.0))
		fmt.Printf("Latency p90:   %d us\n", merged.ValueAtQuantile(90.0))
		fmt.Printf("Latency p95:   %d us\n", merged.ValueAtQuantile(95.0))
		fmt.Printf("Latency p99:   %d us\n", merged.ValueAtQuantile(99.0))
		fmt.Printf("Latency p99.9: %d us\n", merged.ValueAtQuantile(99.9))
		fmt.Printf("Latency max:   %d us\n", merged.Max())
		fmt.Printf("Latency mean:  %.1f us\n", merged.Mean())
		fmt.Printf("Latency stdev: %.1f us\n", merged.StdDev())
	} else {
		fmt.Println("Нет успешных замеров — проверьте вывод ошибок выше.")
	}
}
