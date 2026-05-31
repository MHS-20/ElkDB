package network

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/MHS-20/ElkDB/queries"
	table "github.com/MHS-20/ElkDB/tables"
)

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

// Server listens for incoming ElkWire connections and dispatches each one to
// its own goroutine. Each connection gets its own queries.Session so that
// transactions are isolated between clients.
type Server struct {
	// Addr is the TCP address to listen on, e.g. ":5433".
	Addr string
	// DBPath is the path to the ElkDB data file.
	DBPath string
}

// ListenAndServe starts listening and blocks until l.Close() is called or a
// fatal listen error occurs.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.Addr, err)
	}
	log.Printf("elkdb-server: listening on %s (db: %s)", s.Addr, s.DBPath)
	for {
		conn, err := ln.Accept()
		if err != nil {
			return fmt.Errorf("accept: %w", err)
		}
		go s.handleConn(conn)
	}
}

// handleConn runs the per-connection read loop. It opens a dedicated Session
// for this connection and closes it when the connection drops.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	remote := conn.RemoteAddr().String()
	log.Printf("elkdb-server: new connection from %s", remote)

	session, err := queries.NewSession(s.DBPath)
	if err != nil {
		log.Printf("elkdb-server: [%s] failed to open session: %v", remote, err)
		// Best-effort: tell the client something went wrong then hang up.
		_ = SendError(conn, 0, fmt.Sprintf("server could not open db: %v", err))
		return
	}
	defer func() {
		session.Close()
		log.Printf("elkdb-server: connection closed %s", remote)
	}()

	r := bufio.NewReader(conn)
	for {
		frame, err := ReadFrame(r)
		if err != nil {
			if err != io.EOF {
				log.Printf("elkdb-server: [%s] read error: %v", remote, err)
			}
			return
		}

		switch frame.MsgType {
		case MsgQuery:
			sql, _, err := ReadQuery(r, frame.PayloadLen)
			if err != nil {
				log.Printf("elkdb-server: [%s] malformed query: %v", remote, err)
				_ = SendError(conn, frame.ReqID, "malformed query frame")
				return
			}
			s.execAndRespond(conn, session, frame.ReqID, sql)

		case MsgPing:
			// No payload to read (payloadLen == 0).
			if err := SendPong(conn, frame.ReqID); err != nil {
				log.Printf("elkdb-server: [%s] pong write error: %v", remote, err)
				return
			}

		default:
			log.Printf("elkdb-server: [%s] unknown message type 0x%02x, discarding", remote, frame.MsgType)
			if err := DiscardPayload(r, frame.PayloadLen); err != nil {
				return
			}
		}
	}
}

// execAndRespond runs one SQL string and writes a MsgResult or MsgError back.
func (s *Server) execAndRespond(w io.Writer, session *queries.Session, reqID uint32, sql string) {
	results, err := session.ExecChunk(sql + "\n")
	if err != nil {
		_ = SendError(w, reqID, err.Error())
		return
	}

	// Merge all statement results into a single Result to send back.
	// For multi-statement chunks we accumulate rows and affected counts.
	merged := Result{}
	for _, r := range results {
		merged.Affected += r.Affected
		for _, row := range r.Rows {
			merged.Rows = append(merged.Rows, convertRecord(row))
		}
	}

	if err := SendResult(w, reqID, merged); err != nil {
		log.Printf("elkdb-server: result write error: %v", err)
	}
}

// convertRecord converts a queries.Result row (table.Record) to the network
// Result row type. They are the same underlying type, so this is a no-op
// copy that keeps the network package free of a direct queries import.
func convertRecord(rec table.Record) table.Record { return rec }

// ---------------------------------------------------------------------------
// Convenience: run multiple statements separated by semicolons and collect
// the first non-empty error message for the client.
// ---------------------------------------------------------------------------

// isSelect reports whether the trimmed, upper-cased query starts with SELECT.
func isSelect(sql string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT")
}
