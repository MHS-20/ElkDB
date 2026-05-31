package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/MHS-20/ElkDB/network"
	"github.com/MHS-20/ElkDB/queries"
	table "github.com/MHS-20/ElkDB/tables"
)

func main() {
	// Flags
	remote := flag.String("remote", "", "connect to a running server, e.g. localhost:5433")
	dbPath := flag.String("db", "elkdb.db", "path to the local ElkDB data file (local mode only)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: elkdb [flags]\n\n")
		fmt.Fprintf(os.Stderr, "  Local mode (default): opens the data file directly.\n")
		fmt.Fprintf(os.Stderr, "  Remote mode (-remote): connects to an elkdb-server over TCP.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *remote != "" {
		runRemote(*remote)
	} else {
		runLocal(*dbPath)
	}
}

// ---------------------------------------------------------------------------
// Local mode — identical to the original cli/main.go behaviour
// ---------------------------------------------------------------------------

func runLocal(path string) {
	session, err := queries.NewSession(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", path, err)
		os.Exit(1)
	}
	defer session.Close()

	fmt.Fprintf(os.Stderr, "ElkDB (local) — %s\nType EXIT to quit.\n", path)
	if err := queries.REPL(session, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// Remote mode — REPL over an ElkWire connection
// ---------------------------------------------------------------------------

func runRemote(addr string) {
	conn, err := network.Dial(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	if err := conn.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "server unreachable: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "ElkDB (remote) — %s\nType EXIT to quit.\n", addr)

	var pending strings.Builder
	scanner := bufio.NewScanner(os.Stdin)

	for {
		if pending.Len() > 0 {
			fmt.Fprint(os.Stdout, "  ... > ")
		} else {
			fmt.Fprint(os.Stdout, "elkdb> ")
		}

		if !scanner.Scan() {
			break // EOF
		}
		line := scanner.Text()
		if strings.TrimSpace(strings.ToUpper(line)) == "EXIT" {
			break
		}

		pending.WriteString(line)
		pending.WriteByte('\n')

		// Only send when we have at least one complete statement (ends with ;).
		// This mirrors the local StmtSplitter behaviour so the prompt is consistent.
		sql := pending.String()
		if !containsCompleteStatement(sql) {
			continue
		}
		pending.Reset()

		res, err := conn.Exec(sql)
		if err != nil {
			fmt.Fprintf(os.Stdout, "error: %v\n", err)
			continue
		}
		printResult(os.Stdout, res)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "input error: %v\n", err)
		os.Exit(1)
	}
}

// containsCompleteStatement reports whether s contains at least one
// semicolon outside of a single-quoted string literal.
func containsCompleteStatement(s string) bool {
	inString := false
	for _, ch := range s {
		if ch == '\'' {
			inString = !inString
			continue
		}
		if ch == ';' && !inString {
			return true
		}
	}
	return false
}

// printResult renders a network.Result to w in the same tabular format as
// the local queries.REPL.
func printResult(w *os.File, r network.Result) {
	if len(r.Rows) > 0 {
		fmt.Fprintln(w, strings.Join(r.Rows[0].Cols, "\t"))
		fmt.Fprintln(w, strings.Repeat("-", 40))
		for _, row := range r.Rows {
			parts := make([]string, len(row.Vals))
			for i, v := range row.Vals {
				switch v.Type {
				case table.TypeInt64:
					parts[i] = fmt.Sprintf("%d", v.I64)
				case table.TypeBytes:
					parts[i] = string(v.Str)
				default:
					parts[i] = "?"
				}
			}
			fmt.Fprintln(w, strings.Join(parts, "\t"))
		}
		fmt.Fprintf(w, "(%d rows)\n", len(r.Rows))
	} else if r.Affected > 0 {
		fmt.Fprintf(w, "affected: %d\n", r.Affected)
	} else {
		fmt.Fprintln(w, "ok")
	}
}
