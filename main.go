package main

import (
	"context"
	"fmt"

	drivermock "github.com/alexandrethiel/mongomock/driver"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
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
	fmt.Println("dbs ")
	fmt.Println(cli.ListDatabases(ctx, bson.M{}))
	fmt.Println("cols ")
	fmt.Println(cli.Database("testdb").ListCollectionNames(ctx, bson.M{}))
	fmt.Println(cli.Database("testdb").Collection("testcol").Find(ctx, bson.M{"A": "B"}))
	fmt.Println(cli.Database("testdb").Collection("testcol").InsertOne(ctx, bson.M{"A": "B"}))
	//	fmt.Println(cli.Database(ctx, nil))
}
