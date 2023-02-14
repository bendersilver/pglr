package pglr

import (
	"context"
	"net/url"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

var ctx = context.Background()

// Options -
type Options struct {
	PgURL    string
	SlotName string

	Temporary bool
}

// Conn -
type Conn struct {
	opt *Options
	ch  chan pglogrepl.Message

	conn *pgconn.PgConn
	lsn  pglogrepl.LSN
}

// NewConn -
func NewConn(opt *Options) (*Conn, chan pglogrepl.Message, error) {
	if opt.SlotName == "" {
		opt.SlotName = "pgrpl_slot"
	}

	u, err := url.Parse(opt.PgURL)
	if err != nil {
		return nil, nil, err
	}
	param := url.Values{}
	param.Add("sslmode", "require")
	param.Add("replication", "database")
	param.Add("application_name", opt.SlotName)
	u.RawQuery = param.Encode()

	var c Conn
	c.opt = opt
	c.conn, err = pgconn.Connect(ctx, u.String())
	if err != nil {
		return nil, nil, err
	}

	c.ch = make(chan pglogrepl.Message)

	return &c, c.ch, nil
}

// Close -
func (c *Conn) Close() error {
	if c.conn != nil {
		err := c.DropSlot()
		if err != nil {
			return err
		}
		return c.conn.Close(ctx)
	}
	return nil
}
