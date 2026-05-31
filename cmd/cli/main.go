package main

import (
	"fmt"
	"os"

	"github.com/MHS-20/ElkDB/queries"
)

func main() {
	path := "elkdb.db"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}

	session, err := queries.NewSession(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", path, err)
		os.Exit(1)
	}
	defer session.Close()

	fmt.Fprintf(os.Stderr, "ElkDB — %s\nType EXIT to quit.\n", path)
	if err := queries.REPL(session, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
