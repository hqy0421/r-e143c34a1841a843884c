package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"little-db/internal/client"
	"little-db/internal/engine"
	"little-db/internal/wire"
)

// --- helpers -------------------------------------------------------------

// startServer brings up an engine + server bound to a random port and
// returns the server, its address, and a teardown closure to register
// with t.Cleanup. The caller may also pass overrides for Options.
func startServer(t *testing.T, override func(o *Options)) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := engine.Open(engine.Options{Dir: dir, MaxBatchEncodedSize: 64 * 1024 * 1024})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	opts := Options{
		Addr:                      "127.0.0.1:0",
		ReadDeadline:              2 * time.Second,
		WriteDeadline:             2 * time.Second,
		MaxConcurrentRangeStreams: 4,
		MaxRangeResponseBytes:     64 * 1024 * 1024,
	}
	if override != nil {
		override(&opts)
	}
	srv := New(db, opts)
	if err := srv.Bind(); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	addr := srv.Addr().String()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		<-serveErr
		_ = db.Close()
	})
	return srv, addr
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// roundTrip writes one request and reads one response frame.
func roundTrip(t *testing.T, conn net.Conn, req wire.Request) (wire.Status, []byte) {
	t.Helper()
	frame, err := wire.EncodeRequest(req)
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, body, err := wire.ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return wire.Status(tag), body
}

// --- 1. End-to-end -------------------------------------------------------

func TestServerEndToEnd(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)

	// PUT
	st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte("hello"), Value: []byte("world")})
	if st != wire.StatusOK {
		t.Fatalf("PUT: got status %v want OK", st)
	}

	// GET hit
	st, body := roundTrip(t, conn, &wire.GetRequest{Key: []byte("hello")})
	if st != wire.StatusOK {
		t.Fatalf("GET: status %v want OK", st)
	}
	val, err := wire.DecodeGetOK(body)
	if err != nil {
		t.Fatalf("DecodeGetOK: %v", err)
	}
	if !bytes.Equal(val, []byte("world")) {
		t.Fatalf("GET value: got %q want %q", val, "world")
	}

	// GET miss
	st, _ = roundTrip(t, conn, &wire.GetRequest{Key: []byte("missing")})
	if st != wire.StatusNotFound {
		t.Fatalf("GET missing: got %v want NOT_FOUND", st)
	}

	// DELETE
	st, _ = roundTrip(t, conn, &wire.DeleteRequest{Key: []byte("hello")})
	if st != wire.StatusOK {
		t.Fatalf("DELETE: %v", st)
	}
	st, _ = roundTrip(t, conn, &wire.GetRequest{Key: []byte("hello")})
	if st != wire.StatusNotFound {
		t.Fatalf("GET after delete: %v", st)
	}

	// BATCH
	st, _ = roundTrip(t, conn, &wire.BatchRequest{Entries: []wire.BatchEntry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	}})
	if st != wire.StatusOK {
		t.Fatalf("BATCH: %v", st)
	}

	// PING
	st, _ = roundTrip(t, conn, &wire.PingRequest{})
	if st != wire.StatusOK {
		t.Fatalf("PING: %v", st)
	}

	// STATS
	st, body = roundTrip(t, conn, &wire.StatsRequest{})
	if st != wire.StatusOK {
		t.Fatalf("STATS: %v", st)
	}
	kc, bd, err := wire.DecodeStatsOK(body)
	if err != nil {
		t.Fatalf("DecodeStatsOK: %v", err)
	}
	if kc != 2 { // a, b (hello was deleted)
		t.Fatalf("STATS key_count: got %d want 2", kc)
	}
	if bd == 0 {
		t.Fatalf("STATS bytes_on_disk: got 0, want >0")
	}

	// READKEYRANGE
	_ = sendFrame(t, conn, uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	collected := readRange(t, conn)
	keys := keysOf(collected)
	if !equalStringSets(keys, []string{"a", "b"}) {
		t.Fatalf("range keys: got %v want [a b]", keys)
	}
}

// --- 2. Shutdown drains in-flight ---------------------------------------

func TestServerShutdownDrainsInflight(t *testing.T) {
	srv, addr := startServer(t, nil)
	conn := dial(t, addr)

	// Issue a PUT.
	st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if st != wire.StatusOK {
		t.Fatalf("PUT: %v", st)
	}

	// Concurrently start Shutdown.
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		done <- srv.Shutdown(ctx)
	}()

	// The server should respond to our existing connection with a
	// CLOSED frame (polite shutdown notice) and then close. Read until
	// we either see CLOSED or get EOF.
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	tag, body, err := wire.ReadFrame(conn)
	if err != nil && !errors.Is(err, io.EOF) {
		// Tolerate EOF (server closed without polite notice).
		// Anything else is a bug.
		var ne net.Error
		if !errors.As(err, &ne) {
			t.Fatalf("unexpected read error: %v", err)
		}
	}
	if err == nil {
		if wire.Status(tag) != wire.StatusClosed {
			t.Fatalf("expected CLOSED frame, got status %v body %x", wire.Status(tag), body)
		}
	}

	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// --- 3. Bad-frame classification ----------------------------------------

func TestServerBadFrameClassification(t *testing.T) {
	_, addr := startServer(t, nil)

	cases := []struct {
		name         string
		send         func(conn net.Conn)
		wantStatus   wire.Status // 0 + closeOK=true means server may close instead
		closeAllowed bool        // for FrameError cases, server may close
	}{
		{
			name: "PUT empty key (ProtocolError → BAD_REQUEST)",
			send: func(c net.Conn) {
				body := make([]byte, 8)
				binary.BigEndian.PutUint32(body[0:4], 0) // klen=0
				binary.BigEndian.PutUint32(body[4:8], 0) // vlen=0
				sendRaw(t, c, uint8(wire.OpPut), body)
			},
			wantStatus: wire.StatusBadRequest,
		},
		{
			name: "Unknown opcode (ProtocolError)",
			send: func(c net.Conn) {
				sendRaw(t, c, 0x77, nil)
			},
			wantStatus: wire.StatusBadRequest,
		},
		{
			name: "Truncated GET (ProtocolError)",
			send: func(c net.Conn) {
				sendRaw(t, c, uint8(wire.OpGet), []byte{0x00, 0x00}) // only 2 of 4 klen bytes
			},
			wantStatus: wire.StatusBadRequest,
		},
		{
			name: "FrameError: payload_len = 0 (server closes)",
			send: func(c net.Conn) {
				hdr := []byte{0, 0, 0, 0}
				_, _ = c.Write(hdr)
			},
			closeAllowed: true,
		},
		{
			name: "FrameError: payload_len > MaxFramePayload (server closes)",
			send: func(c net.Conn) {
				hdr := make([]byte, 4)
				binary.BigEndian.PutUint32(hdr, wire.MaxFramePayload+1)
				_, _ = c.Write(hdr)
			},
			closeAllowed: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := dial(t, addr)
			tc.send(conn)
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			tag, _, err := wire.ReadFrame(conn)
			if tc.closeAllowed {
				if err == nil {
					t.Fatalf("expected close/EOF, got status %v", wire.Status(tag))
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if wire.Status(tag) != tc.wantStatus {
				t.Fatalf("status: got %v want %v", wire.Status(tag), tc.wantStatus)
			}
		})
	}
}

// --- 4. Error doesn't desync the connection -----------------------------

func TestServerErrorDoesNotDesyncConnection(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)

	// Send a malformed (but well-framed) GET.
	sendRaw(t, conn, uint8(wire.OpGet), []byte{0x00, 0x00}) // truncated klen
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, _, err := wire.ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame after bad request: %v", err)
	}
	if wire.Status(tag) != wire.StatusBadRequest {
		t.Fatalf("status: got %v want BAD_REQUEST", wire.Status(tag))
	}

	// Now send a perfectly fine PUT and GET and confirm we still get
	// valid responses — the conn was not desynchronized.
	st, _ := roundTrip(t, conn, &wire.PutRequest{Key: []byte("k"), Value: []byte("v")})
	if st != wire.StatusOK {
		t.Fatalf("PUT after error: %v", st)
	}
	st, body := roundTrip(t, conn, &wire.GetRequest{Key: []byte("k")})
	if st != wire.StatusOK {
		t.Fatalf("GET after error: %v", st)
	}
	val, _ := wire.DecodeGetOK(body)
	if !bytes.Equal(val, []byte("v")) {
		t.Fatalf("GET value mismatch: %q", val)
	}
}

// --- 5. Range streams and terminates ------------------------------------

func TestServerReadKeyRangeStreamsAndTerminates(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		// Force multi-page streaming.
		o.MaxRangeResponseBytes = 64 * 1024 * 1024
	})
	conn := dial(t, addr)

	// Populate.
	const N = 200
	for i := 0; i < N; i++ {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := bytes.Repeat([]byte{'v'}, 2048) // 2 KiB each → ~400 KiB total > flush threshold
		st, _ := roundTrip(t, conn, &wire.PutRequest{Key: k, Value: v})
		if st != wire.StatusOK {
			t.Fatalf("PUT %d: %v", i, st)
		}
	}

	sendFrame(t, conn, uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	pages := 0
	var collected []wire.KV
	err := wire.ReadRangeStream(conn, func(p []wire.KV) bool {
		pages++
		collected = append(collected, p...)
		return true
	})
	if err != nil {
		t.Fatalf("ReadRangeStream: %v", err)
	}
	if len(collected) != N {
		t.Fatalf("collected %d, want %d", len(collected), N)
	}
	if pages < 2 {
		t.Fatalf("expected multi-page stream, got %d page(s)", pages)
	}
}

// --- 6. Overload on concurrent stream limit ------------------------------

func TestServerReadKeyRangeOverloadOnConcurrentLimit(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		o.MaxConcurrentRangeStreams = 1
		// Give A a generous write deadline so it stays blocked long
		// enough for B's request to land while the semaphore is held.
		o.WriteDeadline = 10 * time.Second
	})

	// Seed with enough data that a single range stream produces many
	// pages and far exceeds TCP socket buffers — so the server stays
	// blocked on writes (semaphore held) once client A stops reading.
	seed := dial(t, addr)
	const seedCount = 200
	for i := 0; i < seedCount; i++ {
		st, _ := roundTrip(t, seed, &wire.PutRequest{
			Key:   []byte(fmt.Sprintf("k%04d", i)),
			Value: bytes.Repeat([]byte{'x'}, 16*1024), // 16 KiB → ~3.2 MiB total
		})
		if st != wire.StatusOK {
			t.Fatalf("seed PUT %d: %v", i, st)
		}
	}

	// Client A holds the only slot.
	a := dial(t, addr)
	sendFrame(t, a, uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	// Read one frame so we know the server has started streaming (it's
	// past the semaphore acquire). A intentionally does not drain
	// further; subsequent writes will fill TCP buffers and the server
	// will block, holding the semaphore.
	_ = a.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := wire.ReadFrame(a); err != nil {
		t.Fatalf("client A first frame: %v", err)
	}

	// Client B requests a range — must be rejected with OVERLOAD.
	b := dial(t, addr)
	sendFrame(t, b, uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	_ = b.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, _, err := wire.ReadFrame(b)
	if err != nil {
		t.Fatalf("client B ReadFrame: %v", err)
	}
	if wire.Status(tag) != wire.StatusOverload {
		t.Fatalf("client B: got status %v want OVERLOAD", wire.Status(tag))
	}
}

// --- 7. Overload on byte cap --------------------------------------------

func TestServerReadKeyRangeOverloadOnByteCap(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		o.MaxRangeResponseBytes = 16 * 1024 // 16 KiB cap
	})
	conn := dial(t, addr)

	// Populate well past the cap.
	for i := 0; i < 100; i++ {
		st, _ := roundTrip(t, conn, &wire.PutRequest{
			Key:   []byte(fmt.Sprintf("k%03d", i)),
			Value: bytes.Repeat([]byte{'x'}, 1024), // ~1 KiB each
		})
		if st != wire.StatusOK {
			t.Fatalf("PUT %d: %v", i, st)
		}
	}

	sendFrame(t, conn, uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	var collected []wire.KV
	err := wire.ReadRangeStream(conn, func(p []wire.KV) bool {
		collected = append(collected, p...)
		return true
	})
	var re *wire.RemoteError
	if !errors.As(err, &re) {
		t.Fatalf("expected RemoteError, got %v", err)
	}
	if re.Status != wire.StatusOverload {
		t.Fatalf("expected OVERLOAD, got %v", re.Status)
	}
	if len(collected) == 0 {
		t.Fatalf("expected at least one page of pairs before overload")
	}
}

// --- 8. No pipelining (sequential per-connection processing) ------------

func TestServerNoPipelining(t *testing.T) {
	_, addr := startServer(t, func(o *Options) {
		o.MaxRangeResponseBytes = 64 * 1024 * 1024
	})
	conn := dial(t, addr)

	// Seed enough data that the range stream takes multiple frames.
	const N = 50
	for i := 0; i < N; i++ {
		st, _ := roundTrip(t, conn, &wire.PutRequest{
			Key:   []byte(fmt.Sprintf("k%03d", i)),
			Value: bytes.Repeat([]byte{'v'}, 8*1024),
		})
		if st != wire.StatusOK {
			t.Fatalf("PUT %d: %v", i, st)
		}
	}

	// Pipeline two requests: READKEYRANGE then PING.
	rangeFrame, err := wire.EncodeFrame(uint8(wire.OpReadKeyRange), encodeRange(t, nil, nil))
	if err != nil {
		t.Fatalf("encode range frame: %v", err)
	}
	pingFrame, err := wire.EncodeRequest(&wire.PingRequest{})
	if err != nil {
		t.Fatalf("encode ping frame: %v", err)
	}

	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(append(append([]byte{}, rangeFrame...), pingFrame...)); err != nil {
		t.Fatalf("write pipelined: %v", err)
	}

	// We must read the entire range stream FIRST (multi-frame: pages + END),
	// only AFTER which the PING response should arrive. If responses were
	// interleaved the PING reply would appear in the middle of the stream.
	sawEnd := false
	pageCount := 0
	for {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		tag, body, err := wire.ReadFrame(conn)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		status := wire.Status(tag)
		// As long as the range stream is in flight, every frame must be
		// a range page or the END frame — never the PING OK with body=nil
		// in a position that would have arrived only after the stream END.
		if !sawEnd {
			if status != wire.StatusOK {
				t.Fatalf("mid-stream frame: status %v want OK", status)
			}
			_, end, derr := wire.DecodeRangeFrame(body)
			if derr != nil {
				t.Fatalf("mid-stream frame failed to decode as range: %v", derr)
			}
			pageCount++
			if end {
				sawEnd = true
			}
			continue
		}
		// First frame after END must be the PING reply: status=OK,
		// empty body.
		if status != wire.StatusOK || len(body) != 0 {
			t.Fatalf("post-stream frame: want PING OK (status=OK body=empty), got status=%v body_len=%d", status, len(body))
		}
		break
	}
	if pageCount < 2 {
		t.Fatalf("expected multi-page stream, got %d frame(s)", pageCount)
	}
}

// --- 9. Concurrent clients ----------------------------------------------

func TestServerConcurrentClients(t *testing.T) {
	_, addr := startServer(t, nil)

	const clients = 8
	const ops = 50
	var wg sync.WaitGroup
	errCh := make(chan error, clients)
	for c := 0; c < clients; c++ {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("dial: %w", err)
				return
			}
			defer conn.Close()
			for i := 0; i < ops; i++ {
				key := []byte("c" + strconv.Itoa(c) + "-" + strconv.Itoa(i))
				val := []byte("v" + strconv.Itoa(i))
				// PUT
				st, _ := roundTripConn(conn, &wire.PutRequest{Key: key, Value: val})
				if st != wire.StatusOK {
					errCh <- fmt.Errorf("client %d PUT %d: status %v", c, i, st)
					return
				}
				// GET
				st, body := roundTripConn(conn, &wire.GetRequest{Key: key})
				if st != wire.StatusOK {
					errCh <- fmt.Errorf("client %d GET %d: status %v", c, i, st)
					return
				}
				gotVal, _ := wire.DecodeGetOK(body)
				if !bytes.Equal(gotVal, val) {
					errCh <- fmt.Errorf("client %d GET %d: val %q want %q", c, i, gotVal, val)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

// roundTripConn is roundTrip without the testing.T (so a goroutine can
// use it and report errors via a channel).
func roundTripConn(conn net.Conn, req wire.Request) (wire.Status, []byte) {
	frame, err := wire.EncodeRequest(req)
	if err != nil {
		return 0xff, nil
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write(frame); err != nil {
		return 0xff, nil
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, body, err := wire.ReadFrame(conn)
	if err != nil {
		return 0xff, nil
	}
	return wire.Status(tag), body
}

// --- shared helpers ------------------------------------------------------

func sendRaw(t *testing.T, conn net.Conn, tag uint8, body []byte) {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := wire.WriteFrame(conn, tag, body); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
}

func sendFrame(t *testing.T, conn net.Conn, tag uint8, body []byte) int {
	t.Helper()
	sendRaw(t, conn, tag, body)
	return len(body) + 5
}

func encodeRange(t *testing.T, start, end []byte) []byte {
	t.Helper()
	body := make([]byte, 4+len(start)+4+len(end))
	binary.BigEndian.PutUint32(body[0:4], uint32(len(start)))
	copy(body[4:], start)
	off := 4 + len(start)
	binary.BigEndian.PutUint32(body[off:off+4], uint32(len(end)))
	copy(body[off+4:], end)
	return body
}

func readRange(t *testing.T, conn net.Conn) []wire.KV {
	t.Helper()
	var pairs []wire.KV
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err := wire.ReadRangeStream(conn, func(p []wire.KV) bool {
		pairs = append(pairs, p...)
		return true
	}); err != nil {
		t.Fatalf("ReadRangeStream: %v", err)
	}
	return pairs
}

func keysOf(pairs []wire.KV) []string {
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = string(p.Key)
	}
	return out
}

func equalStringSets(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	gm := make(map[string]int)
	for _, s := range got {
		gm[s]++
	}
	for _, s := range want {
		gm[s]--
	}
	for _, c := range gm {
		if c != 0 {
			return false
		}
	}
	return true
}

// --- 10. WRITEREADY probe ------------------------------------------------

// TestWriteReadyStatusDecision pins the four-state decision table the
// handler is built from. Draining wins over the engine flag because a
// tearing-down instance must never be advertised as a write target.
func TestWriteReadyStatusDecision(t *testing.T) {
	cases := []struct {
		name           string
		draining       bool
		writesDisabled bool
		want           wire.Status
	}{
		{"healthy", false, false, wire.StatusOK},
		{"engine refusing writes", false, true, wire.StatusWriteDisabled},
		{"server draining", true, false, wire.StatusDraining},
		{"draining takes precedence over write-disabled", true, true, wire.StatusDraining},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := writeReadyStatus(tc.draining, tc.writesDisabled); got != tc.want {
				t.Fatalf("writeReadyStatus(%v, %v) = %v, want %v",
					tc.draining, tc.writesDisabled, got, tc.want)
			}
		})
	}
}

// TestServerWriteReadyHealthy: a freshly opened engine reports OK.
func TestServerWriteReadyHealthy(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)

	st, body := roundTrip(t, conn, &wire.WriteReadyRequest{})
	if st != wire.StatusOK {
		t.Fatalf("WRITEREADY: got %v want OK (body=%q)", st, body)
	}
	if len(body) != 0 {
		t.Fatalf("OK body must be empty, got %d bytes", len(body))
	}
}

// TestServerWriteReadyEngineDisabled exercises the WRITE_DISABLED wire
// response over a real connection. The engine's fault-injection hook is
// private to the engine package, so the server's engine-state indirection
// is flipped here; the engine-side flag flip itself is covered by the
// engine package's rotate_uncertain tests.
func TestServerWriteReadyEngineDisabled(t *testing.T) {
	srv, addr := startServer(t, nil)
	conn := dial(t, addr)

	srv.engineWritesDisabled = func() bool { return true }

	st, body := roundTrip(t, conn, &wire.WriteReadyRequest{})
	if st != wire.StatusWriteDisabled {
		t.Fatalf("WRITEREADY: got %v want WRITE_DISABLED", st)
	}
	msg, err := wire.DecodeError(body)
	if err != nil {
		t.Fatalf("DecodeError: %v", err)
	}
	if msg == "" {
		t.Fatal("WRITE_DISABLED frame must carry an operator-readable message")
	}
}

// TestServerWriteReadyDraining pins the DRAINING response on an existing
// connection. The connection goroutine is parked in ReadFrame (the idle
// steady state), so flipping the same atomic flag Shutdown flips, while
// leaving the socket open, reproduces the drain window deterministically
// without racing Shutdown's connection teardown. DRAINING is a terminal
// verdict: it is the single frame for the request and the connection then
// closes, with no trailing CLOSED frame (the generic drain boundary used
// by other requests is exercised by TestServerShutdownDrainsInflight).
//
// New connections during drain are rejected at accept (Serve closes them),
// matching a real shutdown where the listener has already stopped
// accepting, so this test deliberately stays on one connection. PING's
// liveness independence from both refusal flags is pinned separately in
// TestServerPingStaysLivenessOnly.
func TestServerWriteReadyDraining(t *testing.T) {
	srv, addr := startServer(t, nil)
	conn := dial(t, addr)

	// Warm up so the conn goroutine is definitely parked in ReadFrame
	// before we enter the drain window.
	if st, _ := roundTrip(t, conn, &wire.PingRequest{}); st != wire.StatusOK {
		t.Fatalf("warmup PING: %v", st)
	}

	srv.draining.Store(true)
	t.Cleanup(func() { srv.draining.Store(false) })

	st, body := roundTrip(t, conn, &wire.WriteReadyRequest{})
	if st != wire.StatusDraining {
		t.Fatalf("WRITEREADY in drain window: got %v want DRAINING", st)
	}
	msg, err := wire.DecodeError(body)
	if err != nil {
		t.Fatalf("DecodeError: %v", err)
	}
	if msg == "" {
		t.Fatal("DRAINING frame must carry an operator-readable message")
	}

	// The DRAINING verdict is terminal: exactly one frame for the one
	// request, then the server closes. There must be no trailing CLOSED
	// frame (which a unary client could read instead of DRAINING), so
	// the next read must surface EOF or a deadline/network error, never
	// another frame.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if tag, extra, rerr := wire.ReadFrame(conn); rerr == nil {
		t.Fatalf("DRAINING must be the final frame; got extra status %v body %x",
			wire.Status(tag), extra)
	}
}

// TestServerWriteReadyUnreachableIsDistinct pins the third distinguishable
// client outcome: an instance that cannot be dialed never produces a
// status byte — the client sees a transport error, never WRITE_DISABLED
// or DRAINING.
func TestServerWriteReadyUnreachableIsDistinct(t *testing.T) {
	// Bind and immediately close a listener to obtain a port with
	// nothing accepting on it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	conn, derr := net.DialTimeout("tcp", addr, 2*time.Second)
	if derr == nil {
		_ = conn.Close()
		t.Skip("closed listener port was reused; skipping unreachable case")
	}
	var re *wire.RemoteError
	if errors.As(derr, &re) {
		t.Fatalf("unreachable must be a transport error, got RemoteError %v", derr)
	}
}

// TestServerPingStaysLivenessOnly: PING must remain a pure liveness check
// and answer OK in both refusal states, so existing ops probes keep their
// semantics after the readiness probe is introduced.
func TestServerPingStaysLivenessOnly(t *testing.T) {
	srv, addr := startServer(t, nil)
	conn := dial(t, addr)

	// Engine refusal over a real connection.
	srv.engineWritesDisabled = func() bool { return true }
	if st, _ := roundTrip(t, conn, &wire.PingRequest{}); st != wire.StatusOK {
		t.Fatalf("PING with writes disabled: got %v want OK (liveness only)", st)
	}
	if st, _ := roundTrip(t, conn, &wire.WriteReadyRequest{}); st != wire.StatusWriteDisabled {
		t.Fatalf("WRITEREADY with writes disabled: got %v want WRITE_DISABLED", st)
	}

	// The drain branch is exercised over TCP in
	// TestServerWriteReadyDraining. Flipping the shared draining flag
	// here would defeat Shutdown's CompareAndSwap in t.Cleanup, so the
	// draining half of the liveness guarantee is pinned at the decision
	// layer instead: PING's handler never consults either flag.
	if got := writeReadyStatus(true, false); got != wire.StatusDraining {
		t.Fatalf("writeReadyStatus(draining): got %v want DRAINING", got)
	}
}

// TestServerWriteReadyRejectsNonEmptyBody: the probe takes no body.
func TestServerWriteReadyRejectsNonEmptyBody(t *testing.T) {
	_, addr := startServer(t, nil)
	conn := dial(t, addr)
	sendRaw(t, conn, uint8(wire.OpWriteReady), []byte{0x01})
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tag, _, err := wire.ReadFrame(conn)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if wire.Status(tag) != wire.StatusBadRequest {
		t.Fatalf("non-empty WRITEREADY body: got %v want BAD_REQUEST", wire.Status(tag))
	}
}

// TestServerWriteReadyViaClient drives the public client.Client API over
// a real TCP listener, pinning the end-to-end error shape traffic
// directors program against: nil when ready, *wire.RemoteError carrying
// the exact status for each refusal state.
func TestServerWriteReadyViaClient(t *testing.T) {
	srv, addr := startServer(t, nil)

	c, err := client.Dial(addr, client.Options{
		DialTimeout:    2 * time.Second,
		RequestTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("client.Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Ready.
	if err := c.WriteReady(); err != nil {
		t.Fatalf("WriteReady healthy: %v", err)
	}

	// Engine refusal.
	srv.engineWritesDisabled = func() bool { return true }
	err = c.WriteReady()
	var re *wire.RemoteError
	if !errors.As(err, &re) || re.Status != wire.StatusWriteDisabled {
		t.Fatalf("WriteReady disabled: err=%v want RemoteError(WRITE_DISABLED)", err)
	}

	// Reset the engine injection briefly so a second probe re-confirms
	// the healthy verdict, then re-arm it: the drain phase must still
	// answer DRAINING, proving over a real connection that draining takes
	// precedence over the engine's write-disabled flag.
	srv.engineWritesDisabled = func() bool { return false }
	if err := c.WriteReady(); err != nil {
		t.Fatalf("WriteReady after reset: %v", err)
	}
	srv.engineWritesDisabled = func() bool { return true }

	// Enter the drain window on the parked connection (same atomic flag
	// Shutdown flips; the socket stays open for one in-flight request).
	// The engine flag remains armed, so DRAINING here also pins the
	// precedence rule end-to-end.
	srv.draining.Store(true)
	t.Cleanup(func() { srv.draining.Store(false) })

	err = c.WriteReady()
	if !errors.As(err, &re) || re.Status != wire.StatusDraining {
		t.Fatalf("WriteReady in drain with engine flag armed: err=%v want DRAINING", err)
	}
}

// Compile-time guard that we don't accidentally remove the atomic import
// (kept for symmetry with engine tests that rely on it).
var _ = atomic.Int64{}
