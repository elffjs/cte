package main

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/lib/pq"
)

var window = time.Second
var limit = 3

func request(db *sql.DB, id string) error {
	tx, err := db.BeginTx(context.TODO(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now()

	var recent int

	if err := tx.QueryRow("SELECT count(1) FROM certs WHERE id = $1 AND time > $2", id, now.Add(-window)).Scan(&recent); err != nil {
		return err
	}

	if recent >= limit {
		log.Printf("Too many requests for id %s.", id)
		return nil
	}

	if _, err := tx.Exec("INSERT INTO certs (id, time) VALUES ($1, $2)", id, now); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func main() {
	db, err := sql.Open("postgres", "postgresql://Kemper@localhost/postgres?sslmode=disable")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 10; i++ {
		var w sync.WaitGroup
		for i := 0; i < 5; i++ {
			w.Add(1)
			go func() {
				if err := request(db, "A"); err != nil {
					// 40001 is serialization_failure, see https://www.postgresql.org/docs/current/errcodes-appendix.html
					if err, ok := err.(*pq.Error); ok && err.Code == "40001" {
						log.Print("Serialization error.")
					} else {
						log.Print("Unexpected error.")
					}
				} else {
					log.Print("Success.")
				}
				w.Done()
			}()
		}
		w.Wait()
		time.Sleep(200 * time.Millisecond)
	}
}
