package server

import (
	"fmt"
	"net"
	"strings"

	"github.com/perfectstay/mongomock/db"
	"github.com/perfectstay/mongomock/protocol"
)

type Server struct {
	listener     net.Listener
	listenerAddr string
	database     *db.Db
}

func NewServer(address string) *Server {
	return &Server{listenerAddr: address, database: &db.Db{}}
}

func (p *Server) Start() error {
	if err := p.createListener(); err != nil {
		return err
	}

	go p.clientAcceptLoop()

	return nil
}

func (p *Server) createListener() error {
	var err error
	if p.listener, err = net.Listen("tcp", p.listenerAddr); err != nil {
		return err
	}

	return nil
}

// clientAcceptLoop accepts new clients and creates a clientServeLoop for each
// new client that connects to the proxy.
func (p *Server) clientAcceptLoop() {
	for {
		c, err := p.listener.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				break
			}
			fmt.Println(err)
			continue
		}

		go p.clientServeLoop(c)
	}
}

// clientServeLoop loops on a single client connected to the proxy and
// dispatches its requests.
func (p *Server) clientServeLoop(c net.Conn) {
	fmt.Printf("client %s connected to %s", c.RemoteAddr(), p)

	defer func() {
		fmt.Printf("client %s disconnected from %s", c.RemoteAddr(), p)
		if err := c.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	for {
		h, err := protocol.ReadMsgHeader(c)
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Printf("handling message %s from %s for %s", h, c.RemoteAddr(), p)
		reply, err := p.database.Handle(h)
		if err != nil {
			fmt.Println(err)
			return
		}
		if reply != nil {
			reply.WriteTo(c)
		}
	}

}

func (p *Server) Stop() error {
	return p.listener.Close()
}
