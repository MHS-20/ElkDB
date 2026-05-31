package main

import (
	"fmt"
	"log"

	"github.com/MHS-20/ElkDB/network"
	table "github.com/MHS-20/ElkDB/tables"
)

func main() {
	db, err := network.Dial("localhost:5433")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create table.
	_, err = db.Exec(`CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id));`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert some rows.
	inserts := []string{
		`INSERT INTO users (id, name, age) VALUES (1, 'alice', 30);`,
		`INSERT INTO users (id, name, age) VALUES (2, 'bob', 25);`,
		`INSERT INTO users (id, name, age) VALUES (3, 'carol', 35);`,
	}
	for _, sql := range inserts {
		res, err := db.Exec(sql)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("inserted: affected=%d\n", res.Affected)
	}

	// Read all rows.
	res, err := db.Exec(`SELECT * FROM users;`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nall users:")
	for _, row := range res.Rows {
		fmt.Println(rowStr(row))
	}

	// Read a single row by primary key.
	res, err = db.Exec(`SELECT * FROM users WHERE id == 2;`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nuser with id=2:")
	for _, row := range res.Rows {
		fmt.Println(rowStr(row))
	}
}

// rowStr formats a record as "col=val col=val ..."
func rowStr(row table.Record) string {
	s := ""
	for i, col := range row.Cols {
		if i > 0 {
			s += "  "
		}
		v := row.Vals[i]
		switch v.Type {
		case table.TypeInt64:
			s += fmt.Sprintf("%s=%d", col, v.I64)
		case table.TypeBytes:
			s += fmt.Sprintf("%s=%s", col, v.Str)
		}
	}
	return s
}
