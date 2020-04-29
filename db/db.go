package db

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/alexandrethiel/mongomock/protocol"
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
		fmt.Println(query.String())
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
			op.AddDocument(map[string]string{"name": "bar"})
		case "system.namespaces":
			op.AddDocument(map[string]string{"name": "bar"})
		}

		return op, nil
	}

	return nil, nil

}
