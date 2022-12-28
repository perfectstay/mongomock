package db

import (
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

// doc= a{b:c} path="a.b" => c
func getValuesAtPath(docInterface interface{}, path string) []interface{} {
	currentValues := []interface{}{docInterface}
	for _, pathItem := range strings.Split(path, ".") {
		newValues := []interface{}{}
		for index, currentItem := range currentValues {
			doc, isDoc := currentItem.(bson.D)
			if isDoc {
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
			} else {
				arrayIndex, err := strconv.Atoi(pathItem)
				if err == nil && arrayIndex == index {
					newValues = append(newValues, currentItem)
				}
			}
		}
		currentValues = newValues
	}
	return currentValues
}

func setValueAtPath(docInterface interface{}, path string, newValue interface{}) interface{} {
	// https://docs.mongodb.com/manual/reference/operator/update/set/
	// can use dot notation
	pathItems := strings.Split(path, ".")
	if doc, isOrderedMap := docInterface.(bson.D); isOrderedMap {
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
			for i := len(pathItems) - 1; i >= 1; i-- {
				newValue = bson.D{bson.E{Key: pathItems[i], Value: newValue}}
			}
			doc = append(doc, bson.E{Key: pathItems[0], Value: newValue})
		}
		return doc
	}
	if doc, isArray := docInterface.(bson.A); isArray {
		updatedIndex, err := strconv.Atoi(pathItems[0])
		if err != nil {
			panic(err)
		}
		if len(pathItems) > 1 {
			doc[updatedIndex] = setValueAtPath(doc[updatedIndex], strings.Join(pathItems[1:], "."), newValue)
		} else {
			doc[updatedIndex] = newValue
		}
		return doc
	}

	panic("type ?? ")
}

func applyUpdate(doc bson.D, update bson.D) bson.D {
	for _, cmdEntry := range update {
		switch cmdEntry.Key {
		case "$set":
			values := cmdEntry.Value.(bson.D)
			for _, updateEntry := range values {
				doc = setValueAtPath(doc, updateEntry.Key, updateEntry.Value).(bson.D)
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
