package pglr

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Run -
func (c *Conn) Run(fn func(pglogrepl.Message)) error {
	err := c.createSlot()
	if err != nil {
		return err
	}

	c.lsn = pglogrepl.LSN(0)
	err = pglogrepl.StartReplication(ctx, c.conn, c.opt.SlotName, c.lsn,
		pglogrepl.StartReplicationOptions{
			Mode: pglogrepl.LogicalReplication,
			PluginArgs: []string{
				"proto_version '1'",
				"publication_names '" + c.opt.SlotName + "'",
			},
		})
	if err != nil {
		return err
	}

	timeout := time.Second * 10
	nextDeadline := time.Now().Add(timeout)
	for {
		if time.Now().After(nextDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(
				ctx,
				c.conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: c.lsn,
				},
			)

			if err != nil {
				return err
			}
			nextDeadline = time.Now().Add(timeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextDeadline)
		rawMsg, err := c.conn.ReceiveMessage(ctx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}

		switch msg := rawMsg.(type) {
		case *pgproto3.ErrorResponse:
			if msg == nil {
				return fmt.Errorf("replication failed: nil message received, should not happen")
			}
			return fmt.Errorf("received Postgres WAL error: %+v", msg)

		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return err
				}
				if pkm.ReplyRequested {
					nextDeadline = time.Time{}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return err
				}

				m, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return err
				}

				// send response
				fn(m)
				c.lsn = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		}
	}
}
