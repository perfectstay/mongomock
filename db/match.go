package db

import (
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
)

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
