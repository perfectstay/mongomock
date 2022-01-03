package db

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func Test_match(t *testing.T) {

	tests := []struct {
		name   string
		doc    bson.D
		filter bson.D
		want   bool
	}{
		{
			name:   "simple no filter",
			doc:    bson.D{{"a", "b"}},
			filter: bson.D{},
			want:   true,
		},
		{
			name:   "simple filter match",
			doc:    bson.D{{"a", "b"}},
			filter: bson.D{{"a", "b"}},
			want:   true,
		},
		{
			name:   "simple filter not match",
			doc:    bson.D{{"a", "b"}},
			filter: bson.D{{"a", "c"}},
			want:   false,
		},
		{
			name: "array match",
			doc: bson.D{{"a", bson.A{
				bson.D{{"b", "c"}},
				bson.D{{"b", "d"}},
			}}},
			filter: bson.D{{"a.b", "d"}},
			want:   true,
		},
		{
			name: "in",
			doc: bson.D{{"a", bson.A{
				bson.D{{"b", "c"}},
				bson.D{{"b", "d"}},
			}}},
			filter: bson.D{{"a.b", bson.D{{"$in", bson.A{"d"}}}}},
			want:   true,
		},
		{
			name:   "exist",
			doc:    bson.D{{"a", "b"}},
			filter: bson.D{{"a", bson.D{{"$exists", true}}}},
			want:   true,
		},
		{
			name:   "not exist",
			doc:    bson.D{{"a", "b"}},
			filter: bson.D{{"c", bson.D{{"$exists", false}}}},
			want:   true,
		},
		{
			name: "or",
			doc:  bson.D{{"a", "b"}},
			filter: bson.D{{
				"$or",
				bson.A{
					bson.D{{"c", "d"}},
					bson.D{{"a", "b"}},
				},
			}},
			want: true,
		},
		{
			name:   "$lte date 1",
			doc:    bson.D{{"a", "2022-07-03"}},
			filter: bson.D{{"a", bson.D{{"$lte", "2022-07-04"}}}},
			want:   true,
		},
		{
			name:   "$lte date 2",
			doc:    bson.D{{"a", "2022-07-03"}},
			filter: bson.D{{"a", bson.D{{"$lte", "2022-07-03"}}}},
			want:   true,
		},
		{
			name:   "$lte date 3",
			doc:    bson.D{{"a", "2022-07-03"}},
			filter: bson.D{{"a", bson.D{{"$lte", "2022-07-02"}}}},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := match(tt.doc, tt.filter)
			if result != tt.want {
				t.Errorf("expected : %v, got : %v", tt.want, result)
			}
		})
	}
}
