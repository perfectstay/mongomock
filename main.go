package main

import (
	"context"
	"fmt"

	drivermock "github.com/perfectstay/mongomock/driver"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	cli, err := mongo.NewClient(drivermock.NewClientOption())
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	err = cli.Connect(ctx)
	if err != nil {
		panic(err)
	}
	//	fmt.Println("dbs ")
	//fmt.Println(cli.ListDatabases(ctx, bson.M{}))
	// fmt.Println("cols ")
	// {
	// 	fmt.Println("----------------")
	// 	displayCursorResult(cli.Database("testdb").ListCollections(ctx, bson.M{}))
	// }
	// {
	// 	fmt.Println("----------------")
	// 	fmt.Println(cli.Database("testdb").Collection("testcol").InsertOne(ctx, bson.M{"A": "B"}))
	// }

	// fmt.Println(cli.Database("testdb").Collection("testcol").FindOneAndUpdate(ctx, bson.M{}, bson.M{"$set": bson.M{"a": "b"}}, options.FindOneAndUpdate().SetUpsert(true)))
	fmt.Println(cli.Database("testdb").Collection("testcol").FindOneAndUpdate(ctx, bson.M{"_id": "aaa"}, bson.M{"$set": bson.M{
		"vals": bson.A{bson.M{"a": "b"}, bson.M{"a": "c"}},
	}}, options.FindOneAndUpdate().SetUpsert(true)))

	{
		fmt.Println("----------------")
		displayCursorResult(cli.Database("testdb").Collection("testcol").Find(ctx, bson.M{"vals.a": bson.M{"$in": bson.A{"c"}}}))
	}

	//	fmt.Println(cli.Database(ctx, nil))
}

func displayCursorResult(cur *mongo.Cursor, err error) {
	if err != nil {
		panic(err)
	}
	result := bson.A{}
	err = cur.All(context.Background(), &result)
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
}
