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

	// Create tables.
	_, err = db.Exec(`CREATE TABLE users (id INT, name TEXT, age INT, PRIMARY KEY (id));`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE orders (id INT, user_id INT, total INT, PRIMARY KEY (id));`)
	if err != nil {
		log.Fatal(err)
	}

	// Insert users.
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

	// Insert orders.
	orderInserts := []string{
		`INSERT INTO orders (id, user_id, total) VALUES (10, 1, 100);`,
		`INSERT INTO orders (id, user_id, total) VALUES (20, 2, 200);`,
		`INSERT INTO orders (id, user_id, total) VALUES (30, 1, 50);`,
	}
	for _, sql := range orderInserts {
		res, err := db.Exec(sql)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("inserted order: affected=%d\n", res.Affected)
	}

	// Read all users.
	res, err := db.Exec(`SELECT * FROM users;`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nall users:")
	for _, row := range res.Rows {
		fmt.Println(rowStr(row))
	}

	// Read a single user by primary key.
	res, err = db.Exec(`SELECT * FROM users WHERE id == 2;`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nuser with id=2:")
	for _, row := range res.Rows {
		fmt.Println(rowStr(row))
	}

	// JOIN users with orders.
	res, err = db.Exec(`SELECT users.name, orders.total FROM users JOIN orders ON users.id == orders.user_id;`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nuser orders (JOIN):")
	for _, row := range res.Rows {
		fmt.Println(rowStr(row))
	}
	fmt.Printf("(%d rows)\n", len(res.Rows))

	// LEFT JOIN: all users including those with no orders.
	res, err = db.Exec(`SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id == orders.user_id;`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nall users with orders (LEFT JOIN):")
	for _, row := range res.Rows {
		fmt.Println(rowStr(row))
	}
	fmt.Printf("(%d rows)\n", len(res.Rows))
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
