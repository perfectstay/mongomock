package db

import (
	"bytes"
	"fmt"
	"reflect"
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

func (d *Db) Handle(h *protocol.MsgHeader) (*protocol.OpReply, error) {
	fmt.Println(h.GetOpCode())
	if h.GetOpCode() == protocol.OpQueryCode {
		query, err := protocol.ReadOpQuery(h, bytes.NewReader(h.Message))
		if err != nil {
			return nil, err
		}
		{
			fmt.Println("query=", query.String())
			bsonRaw, _ := query.Query.ToBSON()
			raw, _ := bson.MarshalExtJSON(bsonRaw, true, true)
			fmt.Println("query=", string(raw))
		}
		op := protocol.NewOpReply(query, 1111111)
		nameParts := strings.Split(query.FullCollectionName.String(), ".")
		dbName := nameParts[0]
		col := strings.Join(nameParts[1:], ".")
		fmt.Printf("db=%v col=%v\n", dbName, col)
		switch col {
		case "$cmd":
			q, _ := query.Query.ToBSON()
			cmd := q[0].Key
			fmt.Printf("cmd=%v\n", cmd)
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
	for _, doc := range col.Documents {
		if !match(doc, q) {
			continue
		}
		reply.AddDocument(doc)
	}
	reply.Documents = reply.Documents[query.NumberToSkip:]
	if query.NumberToReturn != 0 && len(reply.Documents) > int(query.NumberToReturn) {
		reply.Documents = reply.Documents[:query.NumberToReturn]
	}
	fmt.Printf("%s %s : nb=%v\n", dbName, colName, len(reply.Documents))
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
		newDoc = setValueAtPath(newDoc, "_id", id)
		fmt.Printf("upsert ! %s %s %v %v", dbName, colName, id, newDoc)
		col.Documents[id] = newDoc
	}
	return nil
}

// doc= a{b:c} path="a.b" => c
func getValuesAtPath(docInterface interface{}, path string) []interface{} {
	currentValues := []interface{}{docInterface}
	for _, pathItem := range strings.Split(path, ".") {
		newValues := []interface{}{}
		for _, currentItem := range currentValues {
			doc, ok := currentItem.(bson.D)
			if !ok {
				return nil
			}
			for _, existingEntry := range doc {
				if existingEntry.Key == pathItem {
					if itemArray, ok := existingEntry.Value.(bson.A); ok {
						newValues = append(newValues, itemArray...)
					} else {
						newValues = append(newValues, existingEntry.Value)
					}
					break
				}
			}
		}
		currentValues = newValues
	}
	return currentValues
}

func setValueAtPath(docInterface interface{}, path string, newValue interface{}) bson.D {
	pathItems := strings.Split(path, ".")
	doc := docInterface.(bson.D)
	found := false
	for existingIndex, existingEntry := range doc {
		if existingEntry.Key == pathItems[0] {
			if len(pathItems) > 1 {
				doc[existingIndex].Value = setValueAtPath(doc[existingIndex].Value, strings.Join(pathItems[1:], ","), newValue)
			} else {
				doc[existingIndex].Value = newValue
			}

			found = true
			break
		}
	}
	if !found {
		for i := len(pathItems) - 1; i > 1; i-- {
			newValue = bson.D{bson.E{Key: pathItems[i], Value: newValue}}
		}
		doc = append(doc, bson.E{Key: pathItems[0], Value: newValue})
	}
	return doc
}

func applyUpdate(doc bson.D, update bson.D) bson.D {
	for _, cmdEntry := range update {
		switch cmdEntry.Key {
		case "$set":
			values := cmdEntry.Value.(bson.D)
			for _, updateEntry := range values {
				doc = setValueAtPath(doc, updateEntry.Key, updateEntry.Value)
			}
		case "$unset":
			// TODO
			panic("TODO unset")
		default:
			panic(cmdEntry.Key)
		}
	}
	return doc
}

func match(doc bson.D, filter bson.D) bool {
	for _, filterEntry := range filter {
		values := getValuesAtPath(doc, filterEntry.Key)
		match := false
		for _, val := range values {
			if valObj, ok := filterEntry.Value.(bson.D); ok {
				switch valObj[0].Key {
				case "$in":
					operatorValues := valObj[0].Value.(bson.A)
					for _, operatorValue := range operatorValues {
						if reflect.DeepEqual(operatorValue, val) {
							match = true
							break
						}
					}
				}
			}
			if reflect.DeepEqual(filterEntry.Value, val) {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	return true
}
