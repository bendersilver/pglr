package main

import (
	"log"
	"os"

	"github.com/bendersilver/pglr"
	"github.com/jackc/pglogrepl"
)

func main() {
	c, err := pglr.NewConn(&pglr.Options{
		PgURL:     os.Getenv("PG_URL"),
		Temporary: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	c.DropPublication()
	defer c.Close()

	err = c.Run(func(msg pglogrepl.Message) {
		// switch msg.(type) {
		// case *pglogrepl.RelationMessage, *pglogrepl.InsertMessage, *pglogrepl.UpdateMessage, *pglogrepl.DeleteMessage, *pglogrepl.TruncateMessage:
		// 	fn(m)

		// case *pglogrepl.BeginMessage, *pglogrepl.CommitMessage, *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// 	// pass
		// }
		log.Println(msg)
	})
	if err != nil {
		log.Println(err)
	}
}
