package pkg

import (
	"errors"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var (
	TxAbortedError = errors.New("transaction aborted")
)

func (m *Master) mutate(operation Operation, key string, masterDeath MasterDeath, replicaDeaths []ReplicaDeath, f func(r *ReplicaClient, txId string, i int, rd ReplicaDeath) (*bool, error)) (err error) {
	action := operation.String()
	txId := time.Now().String()
	m.log.WriteState(txId, Started)
	m.txs[txId] = Started

	// Send out all mutate requests in parallel. If any abort, send on the channel.
	// Channel must be buffered to allow the non-blocking read in the switch.
	shouldAbort := make(chan int, m.replicaCount)
	log.Println("Master."+action+" asking replicas to "+action+" tx:", txId, "key:", key)
	m.forEachReplica(func(i int, r *ReplicaClient) {
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
		m.log.WriteState(txId, Aborted)
		m.txs[txId] = Aborted
		m.sendAbort(action, txId)
		return TxAbortedError
	default:
		break
	}

	// The transaction is now officially committed
	m.dieIf(masterDeath, MasterDieBeforeLoggingCommitted)
	m.log.WriteState(txId, Committed)
	m.dieIf(masterDeath, MasterDieAfterLoggingCommitted)
	m.txs[txId] = Committed

	log.Println("Master."+action+" asking replicas to commit tx:", txId, "key:", key)
	m.sendAndWaitForCommit(action, txId, replicaDeaths)

	return
}

func (m *Master) dieIf(actual MasterDeath, expected MasterDeath) {
	if !m.didSuicide && actual == expected {
		log.Println("Killing self as requested at", expected)
		m.log.WriteSpecial(KilledSelfMarker)
		os.Exit(1)
	}
}

func (m *Master) Status(args *StatusArgs, reply *StatusResult) (err error) {
	state, ok := m.txs[args.TxId]
	if !ok {
		state = NoState
	}
	reply.State = state
	return nil
}

func (m *Master) Ping(args *PingArgs, reply *GetResult) (err error) {
	reply.Value = args.Key
	return nil
}

func (m *Master) recover() (err error) {
	entries, err := m.log.Read()
	if err != nil {
		return
	}

	m.didSuicide = false
	for _, entry := range entries {
		switch entry.TxId {
		case KilledSelfMarker:
			m.didSuicide = true
			continue
		case FirstRestartAfterSuicideMarker:
			m.didSuicide = false
			continue
		}

		m.txs[entry.TxId] = entry.State
	}

	for txId, state := range m.txs {
		switch state {
		case Started, Aborted:
			log.Println("Aborting tx", txId, "during recovery.")
			m.sendAbort("recover", txId)
		case Committed:
			log.Println("Committing tx", txId, "during recovery.")
			m.sendAndWaitForCommit("recover", txId, make([]ReplicaDeath, m.replicaCount))
		default:
			panic("unhandled default case")
		}
	}

	if m.didSuicide {
		m.log.WriteSpecial(FirstRestartAfterSuicideMarker)
	}
	return
}
func (m *Master) sendAbort(action string, txId string) {
	m.forEachReplica(func(i int, r *ReplicaClient) {
		_, err := r.Abort(txId)
		if err != nil {
			log.Println("Master."+action+" r.Abort:", err)
		}
	})
}

func (m *Master) forEachReplica(f func(i int, r *ReplicaClient)) {
	var wg sync.WaitGroup
	wg.Add(m.replicaCount)
	for i := 0; i < m.replicaCount; i++ {
		go func(i int, r *ReplicaClient) {
			defer wg.Done()
			f(i, r)
		}(i, m.replicas[i])
	}
	wg.Wait()
}

func (m *Master) sendAndWaitForCommit(action string, txId string, replicaDeaths []ReplicaDeath) {
	m.forEachReplica(func(i int, r *ReplicaClient) {
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

func getReplicaDeath(replicaDeaths []ReplicaDeath, n int) ReplicaDeath {
	rd := ReplicaDontDie
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
	err := master.recover()
	if err != nil {
		log.Fatal("Error during recovery: ", err)
	}

	server := rpc.NewServer()
	_ = server.Register(master)
	log.Println("Master listening on port", MasterPort)
	_ = http.ListenAndServe(MasterPort, server)
}
