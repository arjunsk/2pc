package master

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"twopc/pkg/client"
	"twopc/pkg/common"
)

var (
	TxAbortedError = errors.New("transaction aborted")
)

type IMasterTwoPC interface {
	Mutate(operation common.Operation, key string, masterDeath common.MasterDeath, replicaDeaths []common.ReplicaDeath, f func(r *client.ReplicaClient, txId string, i int, rd common.ReplicaDeath) (*bool, error)) (err error)
	SendAbort(action string, txId string)
	SendAndWaitForCommit(action string, txId string, replicaDeaths []common.ReplicaDeath)
	Recover() (err error)
}

func (m *Master) Mutate(operation common.Operation, key string, masterDeath common.MasterDeath, replicaDeaths []common.ReplicaDeath, f func(r *client.ReplicaClient, txId string, i int, rd common.ReplicaDeath) (*bool, error)) (err error) {
	action := operation.String()
	txId := fmt.Sprintf("%d", time.Now().UnixNano())
	m.log.WriteState(txId, common.Started)
	m.txs[txId] = common.Started

	// Send out all Mutate requests in parallel. If any abort, send on the channel.
	// Channel must be buffered to allow the non-blocking read in the switch.
	shouldAbort := make(chan int, m.replicaCount)
	log.Println("Master."+action+" asking replicas to "+action+" tx:", txId, "key:", key)
	m.forEachReplica(func(i int, r *client.ReplicaClient) {
		success, err := f(r, txId, i, getReplicaDeath(replicaDeaths, i))
		if err != nil {
			log.Println("Master."+action+" r.Try"+action+":", err)
		}
		if success == nil || !*success {
			shouldAbort <- 1
		}
	})

	// If at least one replica needed to abort
	select {
	case <-shouldAbort:
		log.Println("Master."+action+" asking replicas to abort tx:", txId, "key:", key)
		m.log.WriteState(txId, common.Aborted)
		m.txs[txId] = common.Aborted
		m.SendAbort(action, txId)
		return TxAbortedError
	default:
		break
	}

	// The transaction is now officially committed
	//TODO: understand this part.
	m.dieIf(masterDeath, common.MasterDieBeforeLoggingCommitted)
	m.log.WriteState(txId, common.Committed)
	m.dieIf(masterDeath, common.MasterDieAfterLoggingCommitted)
	m.txs[txId] = common.Committed

	log.Println("Master."+action+" asking replicas to commit tx:", txId, "key:", key)
	m.SendAndWaitForCommit(action, txId, replicaDeaths)

	return
}

func (m *Master) SendAbort(action string, txId string) {
	m.forEachReplica(func(i int, r *client.ReplicaClient) {
		_, err := r.Abort(txId)
		if err != nil {
			log.Println("Master."+action+" r.Abort:", err)
		}
	})
}

func (m *Master) SendAndWaitForCommit(action string, txId string, replicaDeaths []common.ReplicaDeath) {
	m.forEachReplica(func(i int, r *client.ReplicaClient) {
		for {
			_, err := r.Commit(txId, getReplicaDeath(replicaDeaths, i))
			if err == nil {
				break
			}
			log.Println("Master."+action+" r.Commit:", err)
			time.Sleep(100 * time.Millisecond)
		}
	})
}

func (m *Master) forEachReplica(f func(i int, r *client.ReplicaClient)) {
	var wg sync.WaitGroup
	wg.Add(m.replicaCount)
	for i := 0; i < m.replicaCount; i++ {
		go func(i int, r *client.ReplicaClient) {
			defer wg.Done()
			f(i, r)
		}(i, m.replicas[i])
	}
	wg.Wait()
}

func (m *Master) dieIf(actual common.MasterDeath, expected common.MasterDeath) {
	if !m.didSuicide && actual == expected {
		log.Println("Killing self as requested at", expected)
		m.log.WriteSpecial(common.KilledSelfMarker)
		os.Exit(1)
	}
}

func (m *Master) Recover() (err error) {
	entries, err := m.log.Read()
	if err != nil {
		return
	}

	m.didSuicide = false
	for _, entry := range entries {
		switch entry.TxId {
		case common.KilledSelfMarker:
			m.didSuicide = true
			continue
		case common.FirstRestartAfterSuicideMarker:
			m.didSuicide = false
			continue
		}

		m.txs[entry.TxId] = entry.State
	}

	for txId, state := range m.txs {
		switch state {
		case common.Started, common.Aborted:
			log.Println("Aborting tx", txId, "during recovery.")
			m.SendAbort("Recover", txId)
		case common.Committed:
			log.Println("Committing tx", txId, "during recovery.")
			m.SendAndWaitForCommit("Recover", txId, make([]common.ReplicaDeath, m.replicaCount))
		default:
			panic("unhandled default case")
		}
	}

	if m.didSuicide {
		m.log.WriteSpecial(common.FirstRestartAfterSuicideMarker)
	}
	return
}

func getReplicaDeath(replicaDeaths []common.ReplicaDeath, n int) common.ReplicaDeath {
	rd := common.ReplicaDontDie
	if replicaDeaths != nil && len(replicaDeaths) > n {
		rd = replicaDeaths[n] //ReplicaDontDie
	}
	return rd
}

func RunMaster(replicaCount int) {
	if replicaCount <= 0 {
		log.Fatalln("Replica count must be greater than 0.")
	}

	master := NewMaster(replicaCount)
	err := master.Recover()
	if err != nil {
		log.Fatal("Error during recovery: ", err)
	}

	server := rpc.NewServer()
	_ = server.Register(master)
	log.Println("Master listening on port", common.MasterPort)
	_ = http.ListenAndServe(common.MasterPort, server)
}
