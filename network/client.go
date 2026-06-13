package network

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// Client / SDK
// ---------------------------------------------------------------------------

type response struct {
	result Result
	err    error
}

// Conn is a client connection to an ElkDB server. It supports multiplexed
// requests: multiple Exec / Ping calls may be in-flight concurrently.
// Responses are dispatched to the correct caller by a background reader
// goroutine.
//
// Typical usage:
//
//	c, err := network.Dial("localhost:5433")
//	if err != nil { ... }
//	defer c.Close()
//
//	res, err := c.Exec("INSERT INTO users (id, name) VALUES (1, 'alice');")
//	res, err := c.Exec("SELECT * FROM users;")
type Conn struct {
	conn net.Conn
	r    *bufio.Reader

	wmu     sync.Mutex               // serialises writes to the wire
	pending map[uint32]chan response // reqID → response channel
	pdMu    sync.Mutex               // guards pending
	nextID  uint32                   // atomically incremented request counter

	stopReader chan struct{}
	readerDone chan struct{}
	closeMu    sync.Mutex
	closed     bool
}

// Dial opens a TCP connection to an ElkDB server at addr (e.g. "localhost:5433").
func Dial(addr string) (*Conn, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("elkdb dial %s: %w", addr, err)
	}
	c := &Conn{
		conn:       nc,
		r:          bufio.NewReader(nc),
		pending:    make(map[uint32]chan response),
		stopReader: make(chan struct{}),
		readerDone: make(chan struct{}),
	}
	go c.readerLoop()
	return c, nil
}

// Close shuts down the connection and waits for the background reader to
// finish. All in-flight requests receive a "connection closed" error.
func (c *Conn) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	close(c.stopReader)
	err := c.conn.Close()
	c.closeMu.Unlock()
	<-c.readerDone
	return err
}

// Exec sends a SQL string to the server and returns the merged result.
// Multiple Exec calls may be in-flight concurrently; they are multiplexed
// over the same TCP connection. OCC conflicts are retried transparently
// (up to 20 attempts).
func (c *Conn) Exec(sql string) (Result, error) {
	const maxRetries = 20
	for attempt := 0; attempt < maxRetries; attempt++ {
		reqID := atomic.AddUint32(&c.nextID, 1)
		readOnly := isSelect(sql)

		ch := make(chan response, 1)

		c.pdMu.Lock()
		c.pending[reqID] = ch
		c.pdMu.Unlock()

		c.wmu.Lock()
		err := SendQuery(c.conn, reqID, sql, readOnly)
		c.wmu.Unlock()

		if err != nil {
			c.pdMu.Lock()
			delete(c.pending, reqID)
			c.pdMu.Unlock()
			return Result{}, fmt.Errorf("send query: %w", err)
		}

		r := <-ch
		if r.err != nil {
			// Transparently retry OCC conflicts.
			if attempt < maxRetries-1 && strings.Contains(r.err.Error(), "serialisation conflict") {
				continue
			}
			return Result{}, r.err
		}
		return r.result, nil
	}
	return Result{}, fmt.Errorf("max retries exceeded for OCC conflict")
}

// Ping checks that the server is reachable.
func (c *Conn) Ping() error {
	reqID := atomic.AddUint32(&c.nextID, 1)

	ch := make(chan response, 1)

	c.pdMu.Lock()
	c.pending[reqID] = ch
	c.pdMu.Unlock()

	c.wmu.Lock()
	err := SendPing(c.conn, reqID)
	c.wmu.Unlock()

	if err != nil {
		c.pdMu.Lock()
		delete(c.pending, reqID)
		c.pdMu.Unlock()
		return fmt.Errorf("send ping: %w", err)
	}

	r := <-ch
	return r.err
}

// readerLoop runs in a background goroutine, reads frames from the
// connection, and dispatches them to the waiting Exec / Ping callers.
func (c *Conn) readerLoop() {
	defer close(c.readerDone)

	// Frame-reading goroutine so we can select on stopReader.
	type frameResult struct {
		frame   Frame
		payload []byte
		err     error
	}
	frameCh := make(chan frameResult, 1)
	go func() {
		for {
			frame, err := ReadFrame(c.r)
			if err != nil {
				select {
				case frameCh <- frameResult{err: err}:
				case <-c.stopReader:
				}
				return
			}

			// Read the full payload while still on the read goroutine.
			var payload []byte
			if frame.PayloadLen > 0 {
				payload = make([]byte, frame.PayloadLen)
				if _, err := io.ReadFull(c.r, payload); err != nil {
					select {
					case frameCh <- frameResult{err: err}:
					case <-c.stopReader:
					}
					return
				}
			}

			select {
			case frameCh <- frameResult{
				frame:   Frame{MsgType: frame.MsgType, ReqID: frame.ReqID, PayloadLen: frame.PayloadLen},
				payload: payload,
			}:
			case <-c.stopReader:
				return
			}
		}
	}()

	for {
		select {
		case <-c.stopReader:
			c.failPending(fmt.Errorf("connection closed"))
			return
		case rr := <-frameCh:
			if rr.err != nil {
				c.failPending(fmt.Errorf("connection closed: %w", rr.err))
				return
			}

			frame := rr.frame
			payload := rr.payload

			c.pdMu.Lock()
			ch, ok := c.pending[frame.ReqID]
			if ok {
				delete(c.pending, frame.ReqID)
			}
			c.pdMu.Unlock()

			if !ok {
				continue // unknown reqID, discard
			}

			switch frame.MsgType {
			case MsgResult:
				res, err := decodeResult(payload)
				ch <- response{result: res, err: err}
			case MsgError:
				msg, err := parseErrorPayload(payload)
				if err != nil {
					ch <- response{err: err}
				} else {
					ch <- response{err: fmt.Errorf("%s", msg)}
				}
			case MsgPong:
				ch <- response{}
			default:
				ch <- response{err: fmt.Errorf("unexpected message type 0x%02x", frame.MsgType)}
			}
		}
	}
}

func (c *Conn) failPending(err error) {
	c.pdMu.Lock()
	for _, ch := range c.pending {
		ch <- response{err: err}
	}
	c.pending = nil
	c.pdMu.Unlock()
}
