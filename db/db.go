package db

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/perfectstay/mongomock/protocol"
)

type Db struct {
}

func (d *Db) Handle(h *protocol.MsgHeader) (*protocol.OpReply, error) {
	fmt.Println(h.GetOpCode())
	if h.GetOpCode() == protocol.OpQueryCode {
		query, err := protocol.ReadOpQuery(h, bytes.NewReader(h.Message))
		if err != nil {
			return nil, err
		}
		fmt.Println("query=", query.String())
		op := protocol.NewOpReply(query, 1111111)
		nameParts := strings.Split(query.FullCollectionName.String(), ".")
		db := nameParts[0]
		col := strings.Join(nameParts[1:], ".")
		fmt.Printf("db=%v col=%v\n", db, col)
		switch col {
		case "$cmd":
			q, _ := query.Query.ToBSON()
			cmd := q[0].Key
			fmt.Printf("cmd=%v\n", cmd)
			err = d.handleCmd(db, cmd, query)
			// error handling is done at : x/mongo/driver/errors.go line 351
			if err != nil {
				op.AddDocument(map[string]interface{}{"errmsg": err.Error()})
			} else {
				op.AddDocument(map[string]interface{}{"ok": 1})
			}
		case "system.namespaces":
			op.AddDocument(map[string]string{"name": "bar"})
		}

		return op, nil
	}

	return nil, nil
}

func (d *Db) handleCmd(db, cmd string, query *protocol.OpQuery) error {
	switch cmd {
	case "listDatabases":
		if db != "admin" {
			panic("db != admin : " + db)
		}
		return fmt.Errorf("todo cmd %s", cmd)
	case "findAndModify":
		return fmt.Errorf("todo cmd %s", cmd)
	default:
		return fmt.Errorf("todo cmd %s", cmd)
	}
}
