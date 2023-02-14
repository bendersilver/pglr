package main

import (
	"log"
	"os"

	"github.com/bendersilver/pglr"
)

func main() {
	c, ch, err := pglr.NewConn(&pglr.Options{
		PgURL:     os.Getenv("PG_URL"),
		Temporary: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	c.DropPublication()
	defer c.Close()

	go func() {
		for m := range ch {
			log.Println(m)
		}
	}()

	err = c.Run()
	if err != nil {
		log.Println(err)
	}
}
