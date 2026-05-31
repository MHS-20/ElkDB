package queries

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	table "github.com/MHS-20/ElkDB/tables"
)

// REPL runs an interactive read-eval-print loop reading from in and writing
// to out. It returns when in reaches EOF or the user types EXIT.
func REPL(session *Session, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	for {
		// Show a continuation prompt if a statement is incomplete.
		if session.splitter.HasPending() {
			fmt.Fprint(out, "  ... > ")
		} else {
			fmt.Fprint(out, "elkdb> ")
		}

		if !scanner.Scan() {
			break // EOF
		}
		line := scanner.Text()
		if strings.TrimSpace(strings.ToUpper(line)) == "EXIT" {
			break
		}

		results, err := session.ExecChunk(line + "\n")
		if err != nil {
			fmt.Fprintf(out, "error: %v\n", err)
			continue // don't exit on user errors
		}
		for _, r := range results {
			printResult(out, r)
		}
	}
	return scanner.Err()
}

func printResult(out io.Writer, r Result) {
	if len(r.Rows) > 0 {
		// Print header.
		fmt.Fprintln(out, strings.Join(r.Rows[0].Cols, "\t"))
		fmt.Fprintln(out, strings.Repeat("-", 40))
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
			fmt.Fprintln(out, strings.Join(parts, "\t"))
		}
		fmt.Fprintf(out, "(%d rows)\n", len(r.Rows))
	} else if r.Affected > 0 {
		fmt.Fprintf(out, "affected: %d\n", r.Affected)
	} else {
		fmt.Fprintln(out, "ok")
	}
}
