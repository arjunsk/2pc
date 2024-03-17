package replica

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"strings"
	"twopc/pkg/client"
	"twopc/pkg/common"
	"twopc/pkg/io"
)

type IReplicaAPI interface {
	Get(args *client.ReplicaKeyArgs, reply *client.ReplicaGetResult) (err error)
	TryPut(args *client.TxPutArgs, reply *client.ReplicaActionResult) (err error)
	TryDel(args *client.TxDelArgs, reply *client.ReplicaActionResult) (err error)
	Ping(args *client.ReplicaKeyArgs, reply *client.ReplicaGetResult) (err error)
}

type Replica struct {
	num            int
	committedStore *io.KeyValueStore
	tempStore      *io.KeyValueStore
	txs            map[string]*common.Tx
	lockedKeys     map[string]bool
	log            *io.Logger
	didSuicide     bool
}

func NewReplica(num int) *Replica {
	l := io.NewLogger(fmt.Sprintf("logs/replica%v.txt", num))
	return &Replica{
		num:            num,
		committedStore: io.NewKeyValueStore(fmt.Sprintf("data/replica%v/committed", num)),
		tempStore:      io.NewKeyValueStore(fmt.Sprintf("data/replica%v/temp", num)),
		txs:            make(map[string]*common.Tx),
		lockedKeys:     make(map[string]bool),
		log:            l,
		didSuicide:     false,
	}
}

func (r *Replica) Get(args *client.ReplicaKeyArgs, reply *client.ReplicaGetResult) (err error) {
	val, err := r.committedStore.Get(args.Key)
	log.Printf("Replica.Get: key=%v, value=%v, err=%v\n", args.Key, val, err)
	if err != nil {
		return
	}
	reply.Value = val
	return
}

func (r *Replica) TryPut(args *client.TxPutArgs, reply *client.ReplicaActionResult) (err error) {
	writeToTempStore := func() error { return r.tempStore.Put(r.getTempStoreKey(args.TxId, args.Key), args.Value) }
	log.Printf("Replica.TryPut: key=%v, value=%v, txId=%v, die=%v\n", args.Key, args.Value, args.TxId, args.Die)
	return r.tryMutate(args.Key, args.TxId, args.Die, common.PutOp, writeToTempStore, reply)
}

func (r *Replica) TryDel(args *client.TxDelArgs, reply *client.ReplicaActionResult) (err error) {
	log.Printf("Replica.TryDel: key=%v, txId=%v, die=%v\n", args.Key, args.TxId, args.Die)
	return r.tryMutate(args.Key, args.TxId, args.Die, common.DelOp, nil, reply)
}

func (r *Replica) Ping(args *client.ReplicaKeyArgs, reply *client.ReplicaGetResult) (err error) {
	log.Printf("Replica.Ping: key=%v\n", args.Key)
	reply.Value = args.Key
	return nil
}

func (r *Replica) getTempStoreKey(txId string, key string) string {
	return txId + "__" + key
}

func (r *Replica) parseTempStoreKey(key string) (txId string, txKey string) {
	split := strings.Split(key, "__")
	return split[0], split[1]
}

func RunReplica(num int) {
	replica := NewReplica(num)
	err := replica.Recover()
	if err != nil {
		log.Fatal("Error during recovery: ", err)
	}

	server := rpc.NewServer()
	_ = server.Register(replica)
	log.Println("Replica", num, "listening on port", common.ReplicaPortStart+num)
	_ = http.ListenAndServe(client.GetReplicaHost(num), server)
}
