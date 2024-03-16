package pkg

import (
	"log"
	"math/rand"
	"twopc/pkg/io"
)

type IMaster interface {
	Get(args *GetArgs, reply *GetResult) (err error)
	Put(args *PutArgs, _ *int) (err error)
	Del(args *DelArgs, _ *int) (err error)
	Status(args *StatusArgs, reply *StatusResult) (err error)
	Ping(args *PingArgs, reply *GetResult) (err error)
}

type Master struct {
	replicaCount int
	replicas     []*ReplicaClient
	log          *io.Logger
	txs          map[string]TxState
	didSuicide   bool
}

func NewMaster(replicaCount int) *Master {
	l := io.NewLogger("logs/master.txt")
	replicas := make([]*ReplicaClient, replicaCount)
	for i := 0; i < replicaCount; i++ {
		replicas[i] = NewReplicaClient(GetReplicaHost(i))
	}
	return &Master{
		replicaCount: replicaCount,
		replicas:     replicas,
		log:          l,
		txs:          make(map[string]TxState),
		didSuicide:   false,
	}
}

func (m *Master) Get(args *GetArgs, reply *GetResult) (err error) {
	return m.GetTest(&GetTestArgs{args.Key, -1}, reply)
}

func (m *Master) GetTest(args *GetTestArgs, reply *GetResult) (err error) {
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

func (m *Master) Put(args *PutArgs, _ *int) (err error) {
	var i int
	return m.PutTest(&PutTestArgs{args.Key, args.Value, MasterDontDie, make([]ReplicaDeath, m.replicaCount)}, &i)
}

func (m *Master) PutTest(args *PutTestArgs, _ *int) (err error) {
	return m.mutate(
		PutOp,
		args.Key,
		args.MasterDeath,
		args.ReplicaDeaths,
		func(r *ReplicaClient, txId string, i int, rd ReplicaDeath) (*bool, error) {
			return r.TryPut(args.Key, args.Value, txId, rd)
		})
}

func (m *Master) Del(args *DelArgs, _ *int) (err error) {
	var i int
	return m.DelTest(&DelTestArgs{args.Key, MasterDontDie, make([]ReplicaDeath, m.replicaCount)}, &i)
}

func (m *Master) DelTest(args *DelTestArgs, _ *int) (err error) {
	return m.mutate(
		DelOp,
		args.Key,
		args.MasterDeath,
		args.ReplicaDeaths,
		func(r *ReplicaClient, txId string, i int, rd ReplicaDeath) (*bool, error) {
			return r.TryDel(args.Key, txId, rd)
		})
}
