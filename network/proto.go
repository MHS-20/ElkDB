// Package network implements the ElkWire binary protocol used by the ElkDB
// server and client SDK.
//
// # Frame layout
//
//	┌──────────┬──────────┬────────────┬──────────────────┐
//	│ MsgType  │ ReqID    │ PayloadLen │ Payload          │
//	│ 1 byte   │ 4 bytes  │ 4 bytes    │ PayloadLen bytes │
//	└──────────┴──────────┴────────────┴──────────────────┘
//
// All multi-byte integers are big-endian.
package network

import (
	"encoding/binary"
	"fmt"
	"io"

	table "github.com/MHS-20/ElkDB/tables"
)

// ---------------------------------------------------------------------------
// Message type constants
// ---------------------------------------------------------------------------

const (
	// Client → Server
	MsgQuery byte = 0x01
	MsgPing  byte = 0x02

	// Server → Client
	MsgResult byte = 0x81
	MsgError  byte = 0x82
	MsgPong   byte = 0x83
)

// QueryFlag bits sent in the first byte of a MsgQuery payload.
const (
	// FlagReadOnly is a hint that the query is a SELECT; the server may open
	// a read-only transaction for it.
	FlagReadOnly byte = 0x01
)

// ---------------------------------------------------------------------------
// Frame header
// ---------------------------------------------------------------------------

const headerSize = 9 // 1 + 4 + 4

// header is the fixed-size prefix of every frame.
type header struct {
	MsgType    byte
	ReqID      uint32
	PayloadLen uint32
}

func writeHeader(w io.Writer, h header) error {
	var buf [headerSize]byte
	buf[0] = h.MsgType
	binary.BigEndian.PutUint32(buf[1:5], h.ReqID)
	binary.BigEndian.PutUint32(buf[5:9], h.PayloadLen)
	_, err := w.Write(buf[:])
	return err
}

func readHeader(r io.Reader) (header, error) {
	var buf [headerSize]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return header{}, err
	}
	return header{
		MsgType:    buf[0],
		ReqID:      binary.BigEndian.Uint32(buf[1:5]),
		PayloadLen: binary.BigEndian.Uint32(buf[5:9]),
	}, nil
}

// ---------------------------------------------------------------------------
// SendQuery / ReadQuery
// ---------------------------------------------------------------------------

// SendQuery writes a MsgQuery frame to w.
func SendQuery(w io.Writer, reqID uint32, sql string, readOnly bool) error {
	var flags byte
	if readOnly {
		flags |= FlagReadOnly
	}
	payload := make([]byte, 1+4+len(sql))
	payload[0] = flags
	binary.BigEndian.PutUint32(payload[1:5], uint32(len(sql)))
	copy(payload[5:], sql)

	if err := writeHeader(w, header{MsgQuery, reqID, uint32(len(payload))}); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// ReadQuery reads the payload of a MsgQuery frame (after the header has
// already been consumed). Returns (sql, readOnly, error).
func ReadQuery(r io.Reader, payloadLen uint32) (string, bool, error) {
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return "", false, err
	}
	if len(payload) < 5 {
		return "", false, fmt.Errorf("query payload too short")
	}
	flags := payload[0]
	sqlLen := binary.BigEndian.Uint32(payload[1:5])
	if uint32(len(payload)) < 5+sqlLen {
		return "", false, fmt.Errorf("query payload truncated")
	}
	return string(payload[5 : 5+sqlLen]), flags&FlagReadOnly != 0, nil
}

// ---------------------------------------------------------------------------
// SendResult / ReadResult
// ---------------------------------------------------------------------------

// Result mirrors queries.Result but lives in this package to avoid an import
// cycle (the network layer must not import the queries package; the server
// imports both).
type Result struct {
	Affected int
	Rows     []table.Record
}

// SendResult writes a MsgResult frame to w.
//
// Result payload layout:
//
//	uint32   affected
//	uint32   row_count
//	for each row:
//	  uint16   col_count
//	  for each col:
//	    uint8    name_len
//	    []byte   name
//	    uint8    type  (1=int64, 2=bytes)
//	    if int64:  int64  (8 bytes, big-endian)
//	    if bytes:  uint32 len + []byte data
func SendResult(w io.Writer, reqID uint32, res Result) error {
	payload := encodeResult(res)
	if err := writeHeader(w, header{MsgResult, reqID, uint32(len(payload))}); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func encodeResult(res Result) []byte {
	buf := make([]byte, 0, 64)

	buf = appendUint32(buf, uint32(res.Affected))
	buf = appendUint32(buf, uint32(len(res.Rows)))

	for _, row := range res.Rows {
		buf = appendUint16(buf, uint16(len(row.Cols)))
		for i, col := range row.Cols {
			// column name
			buf = append(buf, byte(len(col)))
			buf = append(buf, col...)
			// value
			val := row.Vals[i]
			switch val.Type {
			case table.TypeInt64:
				buf = append(buf, 0x01)
				buf = appendInt64(buf, val.I64)
			case table.TypeBytes:
				buf = append(buf, 0x02)
				buf = appendUint32(buf, uint32(len(val.Str)))
				buf = append(buf, val.Str...)
			default:
				// zero/unknown type: encode as empty bytes
				buf = append(buf, 0x02)
				buf = appendUint32(buf, 0)
			}
		}
	}
	return buf
}

// ReadResult reads the payload of a MsgResult frame (after the header).
func ReadResult(r io.Reader, payloadLen uint32) (Result, error) {
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Result{}, err
	}
	return decodeResult(payload)
}

func decodeResult(payload []byte) (Result, error) {
	if len(payload) < 8 {
		return Result{}, fmt.Errorf("result payload too short")
	}
	affected := binary.BigEndian.Uint32(payload[0:4])
	rowCount := binary.BigEndian.Uint32(payload[4:8])
	payload = payload[8:]

	rows := make([]table.Record, 0, rowCount)
	for range rowCount {
		if len(payload) < 2 {
			return Result{}, fmt.Errorf("truncated row header")
		}
		colCount := binary.BigEndian.Uint16(payload[0:2])
		payload = payload[2:]

		row := table.Record{
			Cols: make([]string, colCount),
			Vals: make([]table.Value, colCount),
		}
		for j := range colCount {
			// name
			if len(payload) < 1 {
				return Result{}, fmt.Errorf("truncated col name len")
			}
			nameLen := int(payload[0])
			payload = payload[1:]
			if len(payload) < nameLen {
				return Result{}, fmt.Errorf("truncated col name")
			}
			row.Cols[j] = string(payload[:nameLen])
			payload = payload[nameLen:]

			// value
			if len(payload) < 1 {
				return Result{}, fmt.Errorf("truncated value type")
			}
			typ := payload[0]
			payload = payload[1:]
			switch typ {
			case 0x01: // int64
				if len(payload) < 8 {
					return Result{}, fmt.Errorf("truncated int64")
				}
				row.Vals[j] = table.Value{
					Type: table.TypeInt64,
					I64:  int64(binary.BigEndian.Uint64(payload[:8])),
				}
				payload = payload[8:]
			case 0x02: // bytes
				if len(payload) < 4 {
					return Result{}, fmt.Errorf("truncated bytes len")
				}
				dataLen := binary.BigEndian.Uint32(payload[0:4])
				payload = payload[4:]
				if uint32(len(payload)) < dataLen {
					return Result{}, fmt.Errorf("truncated bytes data")
				}
				b := make([]byte, dataLen)
				copy(b, payload[:dataLen])
				row.Vals[j] = table.Value{Type: table.TypeBytes, Str: b}
				payload = payload[dataLen:]
			default:
				return Result{}, fmt.Errorf("unknown value type: 0x%02x", typ)
			}
		}
		rows = append(rows, row)
	}
	return Result{Affected: int(affected), Rows: rows}, nil
}

// ---------------------------------------------------------------------------
// SendError / ReadError
// ---------------------------------------------------------------------------

// SendError writes a MsgError frame to w.
func SendError(w io.Writer, reqID uint32, msg string) error {
	payload := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(payload[0:4], uint32(len(msg)))
	copy(payload[4:], msg)
	if err := writeHeader(w, header{MsgError, reqID, uint32(len(payload))}); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// ReadError reads the payload of a MsgError frame (after the header).
func ReadError(r io.Reader, payloadLen uint32) (string, error) {
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return "", err
	}
	if len(payload) < 4 {
		return "", fmt.Errorf("error payload too short")
	}
	msgLen := binary.BigEndian.Uint32(payload[0:4])
	if uint32(len(payload)) < 4+msgLen {
		return "", fmt.Errorf("error payload truncated")
	}
	return string(payload[4 : 4+msgLen]), nil
}

// ---------------------------------------------------------------------------
// SendPing / SendPong (empty payload)
// ---------------------------------------------------------------------------

func SendPing(w io.Writer, reqID uint32) error {
	return writeHeader(w, header{MsgPing, reqID, 0})
}

func SendPong(w io.Writer, reqID uint32) error {
	return writeHeader(w, header{MsgPong, reqID, 0})
}

// ---------------------------------------------------------------------------
// ReadFrame — generic dispatcher used by both client and server
// ---------------------------------------------------------------------------

// Frame is a decoded frame ready for dispatch.
type Frame struct {
	MsgType    byte
	ReqID      uint32
	PayloadLen uint32
	// Reader is positioned immediately after the header; call the
	// appropriate Read* helper to consume the payload.
}

// ReadFrame reads the fixed-size header and returns a Frame. The caller must
// consume exactly PayloadLen bytes from r before calling ReadFrame again.
func ReadFrame(r io.Reader) (Frame, error) {
	h, err := readHeader(r)
	if err != nil {
		return Frame{}, err
	}
	return Frame{h.MsgType, h.ReqID, h.PayloadLen}, nil
}

// DiscardPayload reads and discards the payload of an unknown frame type.
func DiscardPayload(r io.Reader, payloadLen uint32) error {
	if payloadLen == 0 {
		return nil
	}
	_, err := io.CopyN(io.Discard, r, int64(payloadLen))
	return err
}

// ---------------------------------------------------------------------------
// Little encoding helpers
// ---------------------------------------------------------------------------

func appendUint16(b []byte, v uint16) []byte {
	return append(b, byte(v>>8), byte(v))
}

func appendUint32(b []byte, v uint32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendInt64(b []byte, v int64) []byte {
	u := uint64(v)
	return append(
		b,
		byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u),
	)
}
