package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/MHS-20/ElkDB/network"
)

func main() {
	addr := flag.String("addr", ":5433", "TCP address to listen on")
	dbPath := flag.String("db", "elk.db", "path to the ElkDB data file")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: elkdb-server [flags]\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	srv := &network.Server{
		Addr:   *addr,
		DBPath: *dbPath,
	}
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "elkdb-server: %v\n", err)
		os.Exit(1)
	}
}
