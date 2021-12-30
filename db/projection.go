package db

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func projection(doc bson.D, selection bson.D) bson.D {
	if len(selection) == 0 {
		return doc
	}
	newDoc := bson.D{}
	if _, ok := selection.Map()["_id"]; !ok {
		selection = append(selection, primitive.E{Key: "_id"})
	}
	for _, selection := range selection {
		val, ok := doc.Map()[selection.Key]
		if !ok {
			continue
		}
		newDoc = append(newDoc, primitive.E{Key: selection.Key, Value: val})
	}
	return newDoc
}
