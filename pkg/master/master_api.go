package master

import (
	"log"
	"math/rand"
	"twopc/pkg/client"
	"twopc/pkg/common"
	"twopc/pkg/io"
)

type MasterRpcAPI interface {
	Get(args *client.GetArgs, reply *client.GetResult) (err error)
	Put(args *client.PutArgs, _ *int) (err error)
	Del(args *client.DelArgs, _ *int) (err error)
	Status(args *client.StatusArgs, reply *client.StatusResult) (err error)
	Ping(args *client.PingArgs, reply *client.GetResult) (err error)
}

type Master struct {
	replicaCount int
	replicas     []*client.ReplicaClient
	log          *io.Logger
	txs          map[string]common.TxState
	didSuicide   bool
}

func NewMaster(replicaCount int) *Master {
	l := io.NewLogger("logs/master.txt")
	replicas := make([]*client.ReplicaClient, replicaCount)
	for i := 0; i < replicaCount; i++ {
		replicas[i] = client.NewReplicaClient(client.GetReplicaHost(i))
	}
	return &Master{
		replicaCount: replicaCount,
		replicas:     replicas,
		log:          l,
		txs:          make(map[string]common.TxState),
		didSuicide:   false,
	}
}

func (m *Master) Get(args *client.GetArgs, reply *client.GetResult) (err error) {
	return m.GetTest(&client.GetTestArgs{Key: args.Key, ReplicaNum: -1}, reply)
}

func (m *Master) GetTest(args *client.GetTestArgs, reply *client.GetResult) (err error) {
	log.Println("Master.Get is being called")
	rn := args.ReplicaNum
	if rn < 0 {
		// TODO: Sharding logic.
		rn = rand.Intn(m.replicaCount)
	}
	r, err := m.replicas[rn].Get(args.Key)
	if err != nil {
		log.Printf("Master.Get: request to replica %v for key %v failed\n", rn, args.Key)
		return
	}
	reply.Value = *r
	return nil
}

func (m *Master) Put(args *client.PutArgs, _ *int) (err error) {
	var i int
	return m.PutTest(&client.PutTestArgs{Key: args.Key, Value: args.Value, MasterDeath: common.MasterDontDie, ReplicaDeaths: make([]common.ReplicaDeath, m.replicaCount)}, &i)
}

func (m *Master) PutTest(args *client.PutTestArgs, _ *int) (err error) {
	return m.Mutate(
		common.PutOp,
		args.Key,
		args.MasterDeath,
		args.ReplicaDeaths,
		func(r *client.ReplicaClient, txId string, i int, rd common.ReplicaDeath) (*bool, error) {
			return r.TryPut(args.Key, args.Value, txId, rd)
		})
}

func (m *Master) Del(args *client.DelArgs, _ *int) (err error) {
	var i int
	return m.DelTest(&client.DelTestArgs{Key: args.Key, MasterDeath: common.MasterDontDie, ReplicaDeaths: make([]common.ReplicaDeath, m.replicaCount)}, &i)
}

func (m *Master) DelTest(args *client.DelTestArgs, _ *int) (err error) {
	return m.Mutate(
		common.DelOp,
		args.Key,
		args.MasterDeath,
		args.ReplicaDeaths,
		func(r *client.ReplicaClient, txId string, i int, rd common.ReplicaDeath) (*bool, error) {
			return r.TryDel(args.Key, txId, rd)
		})
}

func (m *Master) Status(args *client.StatusArgs, reply *client.StatusResult) (err error) {
	state, ok := m.txs[args.TxId]
	if !ok {
		state = common.NoState
	}
	reply.State = state
	return nil
}

func (m *Master) Ping(args *client.PingArgs, reply *client.GetResult) (err error) {
	reply.Value = args.Key
	return nil
}
