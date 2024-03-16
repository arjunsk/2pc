package pkg

type Tx struct {
	id    string
	key   string
	op    Operation
	state TxState
}

// ----------------------------------------------------------------------

type ReplicaActionResult struct {
	Success bool
}
type AbortArgs struct {
	TxId string
}
type CommitArgs struct {
	TxId string
	Die  ReplicaDeath
}

type ReplicaGetResult struct {
	Value string
}
type ReplicaKeyArgs struct {
	Key string
}
