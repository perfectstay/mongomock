package db

import (
	"fmt"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func objectCompare(a interface{}, b interface{}) int {
	aDate, aDateOk := a.(primitive.DateTime)
	bDate, bDateOk := b.(primitive.DateTime)
	if aDateOk && bDateOk {
		return int(aDate) - int(bDate)
	}
	return strings.Compare(a.(string), b.(string))
}

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
		matchResult := false
		if valObj, ok := filterEntry.Value.(bson.D); ok {
			operator := valObj[0].Key
			switch operator {
			case "$exists":
				exist := len(values) != 0
				matchResult = valObj[0].Value == exist
			case "$elemMatch":
				for _, val := range values {
					if match(val.(bson.D), valObj[0].Value.(bson.D)) {
						matchResult = true
						break
					}
				}
			case "$lte", "$lt", "$gt", "$gte":
				operatorValue := valObj[0].Value
				for _, val := range values {
					comparisonResult := objectCompare(val, operatorValue)
					switch operator {
					case "$lte":
						matchResult = comparisonResult <= 0
					case "$lt":
						matchResult = comparisonResult < 0
					case "$gt":
						matchResult = comparisonResult > 0
					case "$gte":
						matchResult = comparisonResult >= 0
					}
					if matchResult {
						break
					}
				}
			case "$in":
				for _, val := range values {
					operatorValues := valObj[0].Value.(bson.A)
					for _, operatorValue := range operatorValues {
						if reflect.DeepEqual(operatorValue, val) {
							matchResult = true
							break
						}
					}
				}
			case "$ne":
				matchResult = true
				for _, val := range values {
					if reflect.DeepEqual(filterEntry.Value, val) {
						matchResult = false
						break
					}
				}
			default:
				for _, val := range values {
					if reflect.DeepEqual(filterEntry.Value, val) {
						matchResult = true
						break
					}
				}
			}
		} else {
			for _, val := range values {
				if reflect.DeepEqual(filterEntry.Value, val) {
					matchResult = true
					break
				}
			}
		}
		if !matchResult {
			return false
		}
	}
	return true
}
