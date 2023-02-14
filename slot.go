package pglr

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

// DropPublication -
func (c *Conn) DropPublication() error {
	return c.conn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s;", c.opt.SlotName)).Close()

}

// DropSlot -
func (c *Conn) DropSlot() error {
	return pglogrepl.DropReplicationSlot(ctx,
		c.conn,
		c.opt.SlotName,
		pglogrepl.DropReplicationSlotOptions{Wait: true},
	)
}

// CreateSlot -
func (c *Conn) createSlot() error {
	err := c.conn.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s;", c.opt.SlotName)).Close()
	if err != nil {
		return err
	}
	_, err = pglogrepl.CreateReplicationSlot(ctx,
		c.conn,
		c.opt.SlotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Mode:           pglogrepl.LogicalReplication,
			SnapshotAction: "NOEXPORT_SNAPSHOT",
			Temporary:      true,
		},
	)
	return err
}
