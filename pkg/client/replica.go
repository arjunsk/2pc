package client

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"twopc/pkg/common"
)

type IReplicaClient interface {
	TryPut(key string, value string, txid string, die common.ReplicaDeath) (Success *bool, err error)
	Get(key string) (Value *string, err error)
	TryDel(key string, txid string, die common.ReplicaDeath) (Success *bool, err error)
	Commit(txid string, die common.ReplicaDeath) (Success *bool, err error)
	Abort(txid string) (Success *bool, err error)
}

type ReplicaClient struct {
	host      string
	rpcClient *rpc.Client
}

func NewReplicaClient(host string) *ReplicaClient {
	client := &ReplicaClient{host, nil}
	_ = client.tryConnect()
	return client
}

func (c *ReplicaClient) tryConnect() (err error) {
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

func (c *ReplicaClient) TryPut(key string, value string, txid string, die common.ReplicaDeath) (Success *bool, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.TryPut", &TxPutArgs{key, value, txid, die}, &reply)
	if err != nil {
		log.Println("ReplicaClient.TryPut:", err)
		return
	}

	Success = &reply.Success

	return
}

func (c *ReplicaClient) Get(key string) (Value *string, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaGetResult
	err = c.call("Replica.Get", &ReplicaKeyArgs{key}, &reply)
	if err != nil {
		log.Println("ReplicaClient.Get:", err)
		return
	}

	Value = &reply.Value

	return
}

func (c *ReplicaClient) TryDel(key string, txid string, die common.ReplicaDeath) (Success *bool, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.TryDel", &TxDelArgs{key, txid, die}, &reply)
	if err != nil {
		log.Println("ReplicaClient.TryDel:", err)
		return
	}

	Success = &reply.Success

	return
}

// Commit This is called via RPC by the Master to ask the Replica to commit a transaction.
func (c *ReplicaClient) Commit(txid string, die common.ReplicaDeath) (Success *bool, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.Commit", &CommitArgs{txid, die}, &reply)
	if err != nil {
		log.Println("ReplicaClient.Commit:", err)
		return
	}

	Success = &reply.Success

	return
}

func (c *ReplicaClient) Abort(txid string) (Success *bool, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.Abort", &AbortArgs{txid}, &reply)
	if err != nil {
		log.Println("ReplicaClient.Abort:", err)
		return
	}

	return &reply.Success, nil
}

func (c *ReplicaClient) call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	err = c.rpcClient.Call(serviceMethod, args, reply)
	var opError *net.OpError
	isNetOpError := errors.As(err, &opError)
	if errors.Is(err, rpc.ErrShutdown) || isNetOpError {
		c.rpcClient = nil
	}
	return
}

func GetReplicaHost(replicaNum int) string {
	return fmt.Sprintf("localhost:%v", common.ReplicaPortStart+replicaNum)
}

//----------------------------------------------------------------------

type ReplicaKeyArgs struct {
	Key string
}

type ReplicaGetResult struct {
	Value string
}

type TxPutArgs struct {
	Key   string
	Value string
	TxId  string
	Die   common.ReplicaDeath
}

type TxDelArgs struct {
	Key  string
	TxId string
	Die  common.ReplicaDeath
}
type ReplicaActionResult struct {
	Success bool
}
type CommitArgs struct {
	TxId string
	Die  common.ReplicaDeath
}

type AbortArgs struct {
	TxId string
}
