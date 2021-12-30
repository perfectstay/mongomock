package db

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

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
	// https://docs.mongodb.com/manual/reference/operator/update/set/
	// can use dot notation
	pathItems := strings.Split(path, ".")
	doc := docInterface.(bson.D)
	found := false
	for existingIndex, existingEntry := range doc {
		if existingEntry.Key == pathItems[0] {
			if len(pathItems) > 1 {
				doc[existingIndex].Value = setValueAtPath(doc[existingIndex].Value, strings.Join(pathItems[1:], "."), newValue)
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
