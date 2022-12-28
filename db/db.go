package db

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/perfectstay/mongomock/protocol"
	"go.mongodb.org/mongo-driver/bson"
)

type Collection struct {
	Name      string
	Documents map[interface{}]bson.D
}
type Database struct {
	Name        string
	Collections map[string]*Collection
}
type Db struct {
	Databases map[string]*Database
}

var Trace bool = false

func trace(val ...interface{}) {
	if Trace {
		fmt.Println(val...)
	}
}

func tracef(format string, val ...interface{}) {
	if Trace {
		fmt.Printf(format+"\n", val...)
	}
}

func (d *Db) Handle(h *protocol.MsgHeader) (*protocol.OpReply, error) {
	trace(h.GetOpCode())
	if h.GetOpCode() == protocol.OpQueryCode {
		query, err := protocol.ReadOpQuery(h, bytes.NewReader(h.Message))
		if err != nil {
			return nil, err
		}
		if Trace {
			trace("query=", query.String())
			bsonRaw, _ := query.Query.ToBSON()
			raw, _ := bson.MarshalExtJSON(bsonRaw, true, true)
			trace("query=", string(raw))
		}
		op := protocol.NewOpReply(query, 1111111)
		nameParts := strings.Split(query.FullCollectionName.String(), ".")
		dbName := nameParts[0]
		col := strings.Join(nameParts[1:], ".")
		tracef("db=%v col=%v", dbName, col)
		switch col {
		case "$cmd":
			q, _ := query.Query.ToBSON()
			cmd := q[0].Key
			tracef("cmd=%v", cmd)
			err = d.handleCmd(dbName, cmd, query, op)
			// error handling is done at : x/mongo/driver/errors.go line 351
			if len(op.Documents) == 0 {
				if err != nil {
					op.AddDocument(map[string]interface{}{"errmsg": err.Error()})
				} else {
					op.AddDocument(map[string]interface{}{"ok": 1})
				}
			}
		case "system.namespaces": // list collections, ...
			db := d.Databases[dbName]
			if db != nil {
				for name := range d.Databases[dbName].Collections {
					op.AddDocument(bson.M{"name": name})
				}
			}
		default:
			d.find(dbName, col, query, op)
		}

		return op, nil
	}

	return nil, nil
}

func (d *Db) handleCmd(db, cmd string, query *protocol.OpQuery, reply *protocol.OpReply) error {
	switch cmd {
	case "listDatabases":
		return d.listDatabases(db, query, reply)
	case "insert":
		return d.insert(db, query, reply)
	case "findAndModify":
		return d.findAndModify(db, query, reply)
	case "update":
		return d.update(db, query, reply)
	default:
		return fmt.Errorf("todo cmd %s", cmd)
	}
}

func (d *Db) listDatabases(db string, query *protocol.OpQuery, reply *protocol.OpReply) error {
	if db != "admin" {
		panic("db != admin : " + db)
	}
	// see buildListDatabasesResult from go.mongodb.org/mongo-driver/x/mongo/driver/operation/listDatabases.go
	databases := bson.A{}
	for name, database := range d.Databases {
		databases = append(databases, bson.M{
			"name":       name,
			"sizeOnDisk": 9999,
			"empty":      len(database.Collections) == 0,
		})
	}
	reply.AddDocument(bson.M{
		"ok":        1,
		"totalSize": len(databases),
		"databases": databases})
	return nil
}

func (d *Db) ensureExist(dbName, colName string) (*Database, *Collection) {
	if d.Databases == nil {
		d.Databases = map[string]*Database{}
	}
	db, ok := d.Databases[dbName]
	if !ok {
		db = &Database{Name: dbName, Collections: map[string]*Collection{}}
		d.Databases[dbName] = db
	}

	col, ok := db.Collections[colName]
	if !ok {
		col = &Collection{Name: colName, Documents: map[interface{}]bson.D{}}
		db.Collections[colName] = col
	}

	return db, col
}

func (d *Db) insert(dbName string, query *protocol.OpQuery, reply *protocol.OpReply) error {
	q, _ := query.Query.ToBSON()
	colName := q[0].Value.(string)
	_, col := d.ensureExist(dbName, colName)
	queryMap := q.Map()
	documents := queryMap["documents"].(bson.A)
	for _, docInterface := range documents {
		doc := docInterface.(bson.D)
		col.Documents[doc.Map()["_id"]] = doc
	}

	return nil
}

func (d *Db) update(dbName string, query *protocol.OpQuery, reply *protocol.OpReply) error {
	trace(query)
	q, _ := query.Query.ToBSON()
	trace(q)
	colName := q[0].Value.(string)
	_, _ = d.ensureExist(dbName, colName)
	updates := q.Map()["updates"].(bson.A)
	for _, update := range updates {
		fmt.Println("TODO !! no implemented update.updates[i] !!", update)
	}
	// need to return :
	// N int32 : Number of documents matched.
	// NModified int32 : Number of documents modified.
	// Upserted []Upsert : Information about upserted documents.
	doc := bson.D{
		{"ok", 1},
		{"n", 1},
		{"nModified", 1},
	}
	/* for upsert we should have also

	{"upserted", bson.A{
		bson.D{
			{"index", 0},
			{"_id", 3},
		},
	}},

	*/
	reply.AddDocument(doc)
	return nil
}

func (d *Db) find(dbName, colName string, query *protocol.OpQuery, reply *protocol.OpReply) error {
	db := d.Databases[dbName]
	if db == nil {
		return nil
	}
	col := db.Collections[colName]
	if col == nil {
		return nil
	}
	q, _ := query.Query.ToBSON()
	if queryVal, ok := q.Map()["$query"]; ok {
		q = queryVal.(bson.D)
	}
	for _, doc := range col.Documents {
		if !match(doc, q) {
			continue
		}
		if len(query.ReturnFieldsSelector) != 0 {
			selector, err := query.ReturnFieldsSelector.ToBSON()
			if err != nil {
				return err
			}
			doc = projection(doc, selector)
		}
		reply.AddDocument(doc)
	}
	if len(reply.Documents) < int(query.NumberToSkip) {
		reply.Documents = nil
	} else {
		reply.Documents = reply.Documents[query.NumberToSkip:]
	}
	if query.NumberToReturn > 0 && len(reply.Documents) > int(query.NumberToReturn) {
		reply.Documents = reply.Documents[:query.NumberToReturn]
	}

	tracef("%s %s : nb=%v", dbName, colName, len(reply.Documents))
	reply.NumberReturned = int32(len(reply.Documents))
	return nil
}

func (d *Db) findAndModify(dbName string, query *protocol.OpQuery, reply *protocol.OpReply) error {
	q, _ := query.Query.ToBSON()
	colName := q[0].Value.(string)
	queryParam := q.Map()["query"].(bson.D)
	update := q.Map()["update"].(bson.D)
	upsert := q.Map()["upsert"] == true
	nbUpdated := 0
	{
		db := d.Databases[dbName]
		if db != nil {
			col := db.Collections[colName]
			if col != nil {
				for _, doc := range col.Documents {
					if match(doc, queryParam) {
						newDoc := applyUpdate(doc, update)
						col.Documents[doc.Map()["_id"]] = newDoc
						nbUpdated++
					}
				}
			}
		}
	}
	if nbUpdated == 0 && upsert {
		_, col := d.ensureExist(dbName, colName)
		newDoc := update.Map()["$set"].(bson.D)
		id := queryParam.Map()["_id"]
		if id == nil {
			id = fmt.Sprintf("%v", time.Now().UnixMicro())
		}
		newDoc = setValueAtPath(newDoc, "_id", id).(bson.D)
		tracef("upsert ! %s %s %v %v", dbName, colName, id, newDoc)
		col.Documents[id] = newDoc
	}
	return nil
}
