package pkg

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"
	"twopc/pkg/io"
)

type Replica struct {
	num            int
	committedStore *io.KeyValueStore
	tempStore      *io.KeyValueStore
	txs            map[string]*Tx
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
		txs:            make(map[string]*Tx),
		lockedKeys:     make(map[string]bool),
		log:            l,
		didSuicide:     false,
	}
}

func (r *Replica) recover() (err error) {
	entries, err := r.log.Read()
	if err != nil {
		return
	}

	r.didSuicide = false
	for _, entry := range entries {
		switch entry.TxId {
		case KilledSelfMarker:
			r.didSuicide = true
			continue
		case FirstRestartAfterSuicideMarker:
			r.didSuicide = false
			continue
		}

		if entry.State == Prepared {
			entry.State = r.getStatus(entry.TxId)
			switch entry.State {
			case Aborted:
				log.Println("Aborting transaction during recovery: ", entry.TxId, entry.Key)
				r.abortTx(entry.TxId, RecoveryOp, entry.Key)
			case Committed:
				log.Println("Committing transaction during recovery: ", entry.TxId, entry.Key)
				r.commitTx(entry.TxId, RecoveryOp, entry.Key, ReplicaDontDie)
			default:
				panic("unhandled default case")
			}
		}

		switch entry.State {
		case Started:
		case Prepared:
			// abort
		case Committed:
			r.txs[entry.TxId] = &Tx{entry.TxId, entry.Key, entry.Op, Committed}
		case Aborted:
			r.txs[entry.TxId] = &Tx{entry.TxId, entry.Key, entry.Op, Aborted}
		default:
			panic("unhandled default case")
		}
	}

	err = r.cleanUpTempStore()
	if err != nil {
		return
	}

	if r.didSuicide {
		r.log.WriteSpecial(FirstRestartAfterSuicideMarker)
	}
	return
}

func (r *Replica) abortTx(txId string, op Operation, key string) {
	delete(r.lockedKeys, key)

	switch op {
	case PutOp:
		// We no longer need the temp stored value
		err := r.tempStore.Del(r.getTempStoreKey(txId, key))
		if err != nil {
			fmt.Println("Unable to del val for uncommitted tx:", txId, "key:", key)
		}
		//case DelOp:
		// nothing to undo here
	default:
		panic("unhandled default case")
	}

	r.log.WriteState(txId, Aborted)
	delete(r.txs, txId)
}

func (r *Replica) commitTx(txId string, op Operation, key string, die ReplicaDeath) (err error) {
	delete(r.lockedKeys, key)

	switch op {
	case PutOp:
		val, err := r.tempStore.Get(r.getTempStoreKey(txId, key))
		if err != nil {
			return errors.New(fmt.Sprint("Unable to find val for uncommitted tx:", txId, "key:", key))
		}
		err = r.committedStore.Put(key, val)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to put committed val for tx:", txId, "key:", key))
		}
	case DelOp:
		err = r.committedStore.Del(key)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to commit del val for tx:", txId, "key:", key))
		}
	default:
		panic("unhandled default case")
	}

	r.log.WriteState(txId, Committed)
	delete(r.txs, txId)

	// Delete the temp data only after committed, in case we crash after deleting, but before committing
	if op == PutOp {
		err = r.tempStore.Del(r.getTempStoreKey(txId, key))
		r.dieIf(die, ReplicaDieAfterDeletingFromTempStore)
		if err != nil {
			fmt.Println("Unable to del committed val for tx:", txId, "key:", key)
		}
	}

	r.dieIf(die, ReplicaDieAfterLoggingCommitted)
	return nil
}

func (r *Replica) dieIf(actual ReplicaDeath, expected ReplicaDeath) {
	if !r.didSuicide && actual == expected {
		log.Println("Killing self as requested at", expected)
		r.log.WriteSpecial(KilledSelfMarker)
		os.Exit(1)
	}
}

func (r *Replica) cleanUpTempStore() (err error) {
	keys, err := r.tempStore.List()
	if err != nil {
		return
	}

	for _, key := range keys {
		txId, _ := r.parseTempStoreKey(key)
		tx, ok := r.txs[txId]
		if !ok || tx.state != Prepared {
			println("Cleaning up temp key ", key)
			err = r.tempStore.Del(key)
			if err != nil {
				return
			}
		}
	}
	return nil
}

// getStatus is only used during recovery to check the status from the Master
func (r *Replica) getStatus(txId string) TxState {
	client := NewMasterClient(MasterPort)
	for {
		state, err := client.Status(txId)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return *state
	}

	return NoState
}

func RunReplica(num int) {
	replica := NewReplica(num)
	err := replica.recover()
	if err != nil {
		log.Fatal("Error during recovery: ", err)
	}

	server := rpc.NewServer()
	_ = server.Register(replica)
	log.Println("Replica", num, "listening on port", ReplicaPortStart+num)
	_ = http.ListenAndServe(GetReplicaHost(num), server)
}

//------------------------------------------------------------

func (r *Replica) getTempStoreKey(txId string, key string) string {
	return txId + "__" + key
}

func (r *Replica) parseTempStoreKey(key string) (txId string, txKey string) {
	split := strings.Split(key, "__")
	return split[0], split[1]
}
