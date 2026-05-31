package network

import (
	"bufio"
	"fmt"
	"net"
	"sync/atomic"
)

// ---------------------------------------------------------------------------
// Client / SDK
// ---------------------------------------------------------------------------

// Conn is a client connection to an ElkDB server. It is safe to use from a
// single goroutine; for concurrent use create one Conn per goroutine or add
// your own locking.
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
	conn   net.Conn
	r      *bufio.Reader
	nextID uint32 // atomically incremented request counter
}

// Dial opens a TCP connection to an ElkDB server at addr (e.g. "localhost:5433").
func Dial(addr string) (*Conn, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("elkdb dial %s: %w", addr, err)
	}
	return &Conn{
		conn: nc,
		r:    bufio.NewReader(nc),
	}, nil
}

// Close shuts down the connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// Exec sends a SQL string to the server and returns the merged result.
// The sql string may contain multiple semicolon-separated statements.
func (c *Conn) Exec(sql string) (Result, error) {
	reqID := atomic.AddUint32(&c.nextID, 1)
	readOnly := isSelect(sql)

	if err := SendQuery(c.conn, reqID, sql, readOnly); err != nil {
		return Result{}, fmt.Errorf("send query: %w", err)
	}
	return c.readResponse(reqID)
}

// Ping checks that the server is reachable.
func (c *Conn) Ping() error {
	reqID := atomic.AddUint32(&c.nextID, 1)
	if err := SendPing(c.conn, reqID); err != nil {
		return fmt.Errorf("send ping: %w", err)
	}
	frame, err := ReadFrame(c.r)
	if err != nil {
		return fmt.Errorf("read pong: %w", err)
	}
	if frame.MsgType != MsgPong {
		_ = DiscardPayload(c.r, frame.PayloadLen)
		return fmt.Errorf("expected pong, got 0x%02x", frame.MsgType)
	}
	return nil
}

// readResponse reads the next frame from the server, expecting it to match
// reqID, and returns the decoded result or error.
func (c *Conn) readResponse(reqID uint32) (Result, error) {
	frame, err := ReadFrame(c.r)
	if err != nil {
		return Result{}, fmt.Errorf("read response: %w", err)
	}
	if frame.ReqID != reqID {
		// Out-of-order response — the protocol is currently synchronous so
		// this should never happen. Discard and surface as an error.
		_ = DiscardPayload(c.r, frame.PayloadLen)
		return Result{}, fmt.Errorf("unexpected reqID %d (want %d)", frame.ReqID, reqID)
	}

	switch frame.MsgType {
	case MsgResult:
		res, err := ReadResult(c.r, frame.PayloadLen)
		if err != nil {
			return Result{}, fmt.Errorf("decode result: %w", err)
		}
		return res, nil

	case MsgError:
		msg, err := ReadError(c.r, frame.PayloadLen)
		if err != nil {
			return Result{}, fmt.Errorf("decode error: %w", err)
		}
		return Result{}, fmt.Errorf("%s", msg)

	default:
		_ = DiscardPayload(c.r, frame.PayloadLen)
		return Result{}, fmt.Errorf("unexpected message type 0x%02x", frame.MsgType)
	}
}
