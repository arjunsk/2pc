package replica

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"
	"twopc/pkg/client"
	"twopc/pkg/common"
)

type IReplica2PC interface {
	Commit(args *client.CommitArgs, reply *client.ReplicaActionResult) (err error)
	Abort(args *client.AbortArgs, reply *client.ReplicaActionResult) (err error)

	Recover() (err error)
}

func (r *Replica) Commit(args *client.CommitArgs, reply *client.ReplicaActionResult) (err error) {
	r.dieIf(args.Die, common.ReplicaDieBeforeProcessingCommit)

	reply.Success = false

	txId := args.TxId

	tx, hasTx := r.txs[txId]
	if !hasTx {
		// Error! We've never heard of this transaction
		log.Println("Received commit for unknown transaction:", txId)
		return errors.New(fmt.Sprint("Received commit for unknown transaction:", txId))
	}

	_, keyLocked := r.lockedKeys[tx.Key]
	if !keyLocked {
		// Shouldn't happen, key is unlocked
		log.Println("Received commit for transaction with unlocked key:", txId)
	}

	switch tx.State {
	case common.Prepared:
		err = r.commitTx(txId, tx.Op, tx.Key, args.Die)
	default:
		log.Println("Received commit for transaction in state ", tx.State.String())
	}

	if err == nil {
		reply.Success = true
	}
	return
}

func (r *Replica) Abort(args *client.AbortArgs, reply *client.ReplicaActionResult) (err error) {
	reply.Success = false

	txId := args.TxId

	tx, hasTx := r.txs[txId]
	if !hasTx {
		// Shouldn't happen, we've never heard of this transaction
		return errors.New(fmt.Sprint("Received abort for unknown transaction:", txId))
	}

	_, keyLocked := r.lockedKeys[tx.Key]
	if !keyLocked {
		// Shouldn't happen, key is unlocked
		log.Println("Received abort for transaction with unlocked key:", txId)
	}

	switch tx.State {
	case common.Prepared:
		r.abortTx(txId, tx.Op, tx.Key)
	default:
		log.Println("Received abort for transaction in state ", tx.State.String())
	}

	reply.Success = true
	return nil
}

func (r *Replica) tryMutate(key string, txId string, die common.ReplicaDeath, op common.Operation, f func() error, reply *client.ReplicaActionResult) (err error) {
	r.dieIf(die, common.ReplicaDieBeforeProcessingMutateRequest)
	reply.Success = false

	r.txs[txId] = &common.Tx{Id: txId, Key: key, Op: op, State: common.Started}

	if _, ok := r.lockedKeys[key]; ok {
		// Key is currently being modified, Abort
		log.Println("Received", op.String(), "for locked key:", key, "in tx:", txId, " Aborting")
		r.txs[txId].State = common.Aborted
		r.log.WriteState(txId, common.Aborted)
		return nil
	}

	r.lockedKeys[key] = true

	if f != nil {
		err = f()
		if err != nil {
			log.Println("Unable to", op.String(), "uncommited val for transaction:", txId, "key:", key, ", Aborting")
			r.txs[txId].State = common.Aborted
			r.log.WriteState(txId, common.Aborted)
			delete(r.lockedKeys, key)
			return
		}
	}

	r.txs[txId].State = common.Prepared
	r.log.WriteOp(txId, common.Prepared, op, key)
	reply.Success = true

	r.dieIf(die, common.ReplicaDieAfterLoggingPrepared)

	return
}

func (r *Replica) abortTx(txId string, op common.Operation, key string) {
	delete(r.lockedKeys, key)

	switch op {
	case common.PutOp:
		// We no longer need the temp stored value
		err := r.tempStore.Del(r.getTempStoreKey(txId, key))
		if err != nil {
			fmt.Println("Unable to del val for uncommitted tx:", txId, "key:", key)
		}
	case common.DelOp:
		//nothing to undo here
	default:
		panic("unhandled default case")
	}

	r.log.WriteState(txId, common.Aborted)
	delete(r.txs, txId)
}

func (r *Replica) commitTx(txId string, op common.Operation, key string, die common.ReplicaDeath) (err error) {
	delete(r.lockedKeys, key)

	switch op {
	case common.PutOp:
		val, err := r.tempStore.Get(r.getTempStoreKey(txId, key))
		if err != nil {
			return errors.New(fmt.Sprint("Unable to find val for uncommitted tx:", txId, "key:", key))
		}
		err = r.committedStore.Put(key, val)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to put committed val for tx:", txId, "key:", key))
		}
	case common.DelOp:
		err = r.committedStore.Del(key)
		if err != nil {
			return errors.New(fmt.Sprint("Unable to commit del val for tx:", txId, "key:", key))
		}
	default:
		panic("unhandled default case")
	}

	r.log.WriteState(txId, common.Committed)
	delete(r.txs, txId)

	// Delete the temp data only after committed, in case we crash after deleting, but before committing
	if op == common.PutOp {
		err = r.tempStore.Del(r.getTempStoreKey(txId, key))
		r.dieIf(die, common.ReplicaDieAfterDeletingFromTempStore)
		if err != nil {
			fmt.Println("Unable to del committed val for tx:", txId, "key:", key)
		}
	}

	r.dieIf(die, common.ReplicaDieAfterLoggingCommitted)
	return nil
}

func (r *Replica) Recover() (err error) {
	entries, err := r.log.Read()
	if err != nil {
		return
	}

	r.didSuicide = false
	for _, entry := range entries {
		switch entry.TxId {
		case common.KilledSelfMarker:
			r.didSuicide = true
			continue
		case common.FirstRestartAfterSuicideMarker:
			r.didSuicide = false
			continue
		}

		if entry.State == common.Prepared {
			entry.State = r.getStatus(entry.TxId)
			switch entry.State {
			case common.Aborted:
				log.Println("Aborting transaction during recovery: ", entry.TxId, entry.Key)
				r.abortTx(entry.TxId, common.RecoveryOp, entry.Key)
			case common.Committed:
				log.Println("Committing transaction during recovery: ", entry.TxId, entry.Key)
				_ = r.commitTx(entry.TxId, common.RecoveryOp, entry.Key, common.ReplicaDontDie)
			default:
				panic("unhandled default case")
			}
		}

		switch entry.State {
		case common.Started:
		case common.Prepared:
			// abort
		case common.Committed:
			r.txs[entry.TxId] = &common.Tx{entry.TxId, entry.Key, entry.Op, common.Committed}
		case common.Aborted:
			r.txs[entry.TxId] = &common.Tx{entry.TxId, entry.Key, entry.Op, common.Aborted}
		default:
			panic("unhandled default case")
		}
	}

	err = r.cleanUpTempStore()
	if err != nil {
		return
	}

	if r.didSuicide {
		r.log.WriteSpecial(common.FirstRestartAfterSuicideMarker)
	}
	return
}

func (r *Replica) getStatus(txId string) common.TxState {
	c := client.NewMasterClient(common.MasterPort)
	i := 0
	for {
		i++
		state, err := c.Status(txId)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if i > 3 {
			log.Println("Master is down")
			return common.NoState
		}
		return *state
	}
}

func (r *Replica) dieIf(actual common.ReplicaDeath, expected common.ReplicaDeath) {
	if !r.didSuicide && actual == expected {
		log.Println("Killing self as requested at", expected)
		r.log.WriteSpecial(common.KilledSelfMarker)
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
		if !ok || tx.State != common.Prepared {
			println("Cleaning up temp key ", key)
			err = r.tempStore.Del(key)
			if err != nil {
				return
			}
		}
	}
	return nil
}
