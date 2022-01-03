package db

import (
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
)

func match(doc bson.D, filter bson.D) bool {
	for _, filterEntry := range filter {
		if filterEntry.Key == "$or" {
			filterValues := filterEntry.Value.(bson.A)
			matched := false
			for _, filterVal := range filterValues {
				if match(doc, filterVal.(bson.D)) {
					matched = true
					break
				}
			}
			if !matched {
				fmt.Printf("not match %+v\n", filterEntry)
				return false
			}
			continue
		}
		values := getValuesAtPath(doc, filterEntry.Key)
		match := false
		if valObj, ok := filterEntry.Value.(bson.D); ok {
			operator := valObj[0].Key
			switch operator {
			case "$exists":
				exist := len(values) != 0
				match = valObj[0].Value == exist
			case "$lte", "$lt", "$gt", "$gte":
				operatorValue := valObj[0].Value
				for _, val := range values {
					comparisonResult := strings.Compare(val.(string), operatorValue.(string))
					switch operator {
					case "$lte":
						match = comparisonResult <= 0
					case "$lt":
						match = comparisonResult < 0
					case "$gt":
						match = comparisonResult > 0
					case "$gte":
						match = comparisonResult >= 0
					}
					if match {
						break
					}
				}
			case "$in":
				for _, val := range values {
					operatorValues := valObj[0].Value.(bson.A)
					for _, operatorValue := range operatorValues {
						if reflect.DeepEqual(operatorValue, val) {
							match = true
							break
						}
					}
				}
			case "$ne":
				match = true
				for _, val := range values {
					if reflect.DeepEqual(filterEntry.Value, val) {
						match = false
						break
					}
				}
			default:
				for _, val := range values {
					if reflect.DeepEqual(filterEntry.Value, val) {
						match = true
						break
					}
				}
			}
		} else {
			for _, val := range values {
				if reflect.DeepEqual(filterEntry.Value, val) {
					match = true
					break
				}
			}
		}
		if !match {
			return false
		}
	}
	return true
}
