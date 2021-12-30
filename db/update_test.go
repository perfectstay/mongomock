package db

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func Test_setValueAtPath(t *testing.T) {

	tests := []struct {
		name     string
		doc      interface{}
		path     string
		newValue interface{}
		expected interface{}
	}{
		{
			name:     "simple update",
			doc:      bson.D{{"a", "b"}},
			path:     "a",
			newValue: "c",
			expected: bson.D{{"a", "c"}},
		},
		{
			name:     "simple add",
			doc:      bson.D{},
			path:     "a",
			newValue: "b",
			expected: bson.D{{"a", "b"}},
		},
		{
			name:     "deep add",
			doc:      bson.D{},
			path:     "a.b",
			newValue: "c",
			expected: bson.D{{"a", bson.D{{"b", "c"}}}},
		},
		{
			name: "set in an array",
			doc: bson.D{{
				"a", bson.A{
					bson.D{{"b", "c"}},
				},
			}},
			path:     "a.0.b",
			newValue: "d",
			expected: bson.D{{
				"a", bson.A{
					bson.D{{"b", "d"}},
				},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := setValueAtPath(tt.doc, tt.path, tt.newValue)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("expected : %v, got : %v", tt.expected, result)
			}
		})
	}
}
