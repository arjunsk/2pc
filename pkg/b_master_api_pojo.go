package pkg

type PingArgs struct {
	Key string
}

type GetArgs struct {
	Key string
}

type GetTestArgs struct {
	Key        string
	ReplicaNum int
}

type GetResult struct {
	Value string
}

// ----------------------------------------------------------------------

type PutArgs struct {
	Key   string
	Value string
}

type PutTestArgs struct {
	Key           string
	Value         string
	MasterDeath   MasterDeath
	ReplicaDeaths []ReplicaDeath
}

// ----------------------------------------------------------------------

type DelArgs struct {
	Key string
}
type DelTestArgs struct {
	Key           string
	MasterDeath   MasterDeath
	ReplicaDeaths []ReplicaDeath
}

// ----------------------------------------------------------------------

type StatusArgs struct {
	TxId string
}

type StatusResult struct {
	State TxState
}
