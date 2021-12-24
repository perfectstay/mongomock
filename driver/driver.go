package driver

import (
	"bytes"
	"context"
	"fmt"

	"github.com/perfectstay/mongomock/db"
	"github.com/perfectstay/mongomock/protocol"
	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver"
)

type TestConnection struct {
	database     *db.Db
	responseChan chan ([]byte)
}

func (c TestConnection) WriteWireMessage(ctx context.Context, b []byte) error {
	fmt.Println("out >> ", string(b))
	h, err := protocol.ReadMsgHeader(bytes.NewReader(b))
	if err != nil {
		return err
	}
	reply, err := c.database.Handle(h)
	if err != nil {
		return err
	}
	if reply != nil {
		var buf bytes.Buffer
		err = reply.WriteTo(&buf)
		if err != nil {
			return err
		}
		c.responseChan <- buf.Bytes()
	}

	return nil
}

func (c TestConnection) ReadWireMessage(ctx context.Context, dst []byte) ([]byte, error) {
	fmt.Println("read ", len(dst))
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case data := <-c.responseChan:
		fmt.Println("data ", len(data))
		return data, nil
	}
	return nil, nil
}

func (c TestConnection) Description() description.Server {
	fmt.Println("Description")
	return description.Server{
		MaxBatchCount:   999999,
		MaxDocumentSize: 999999,
		MaxMessageSize:  999999,
	}
}

func (c TestConnection) Close() error {
	fmt.Println("Close")
	return nil
}

func (c TestConnection) ID() string {
	fmt.Println("ID")
	return "testCnxId"
}

func (c TestConnection) Address() address.Address {
	fmt.Println("Address")
	return "testAddress"
}

func (c TestConnection) ServerConnectionID() *int32 {
	var id int32 = 0
	return &id
}

func (c TestConnection) Stale() bool {
	return false
}

func NewConnection() driver.Connection {
	return &TestConnection{
		database:     &db.Db{},
		responseChan: make(chan ([]byte), 1),
	}
}

func NewDeployement() driver.Deployment {
	return driver.SingleConnectionDeployment{
		C: NewConnection(),
	}
}

func NewClientOption() *options.ClientOptions {
	return &options.ClientOptions{
		Deployment: NewDeployement(),
	}
}
