package pkg

import (
	"errors"
	"log"
	"net"
	"net/rpc"
)

type MasterClient struct {
	host      string
	rpcClient *rpc.Client
}

func NewMasterClient(host string) *MasterClient {
	client := &MasterClient{host, nil}
	client.tryConnect()
	return client
}
func (c *MasterClient) tryConnect() (err error) {
	if c.rpcClient != nil {
		return
	}

	rpcClient, err := rpc.DialHTTP("tcp", c.host)
	if err != nil {
		return
	}
	c.rpcClient = rpcClient
	return
}

func (c *MasterClient) Status(txid string) (State *TxState, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply StatusResult
	err = c.call("Master.Status", &StatusArgs{txid}, &reply)
	if err != nil {
		log.Println("MasterClient.Status:", err)
		return
	}

	State = &reply.State

	return
}

func (c *MasterClient) call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	err = c.rpcClient.Call(serviceMethod, args, reply)
	var opError *net.OpError
	isNetOpError := errors.As(err, &opError)
	if errors.Is(err, rpc.ErrShutdown) || isNetOpError {
		c.rpcClient = nil
	}
	return
}
