## Two-Phase Commit

Part of [Distributed Algorithms](https://en.wikipedia.org/wiki/Distributed_algorithm)


## Reference:

- [gotwopc](https://github.com/ianobermiller/gotwopc): Replica + Working
- [committer](https://github.com/vadiminshakov/committer): Hooks, More OOPs, Didn't read in detail
- [2PC-TextBook](https://martinfowler.com/articles/patterns-of-distributed-systems/two-phase-commit.html): Good
- [Distributed Txn](https://www.youtube.com/watch?v=PaMDNhVD-0U&list=PLzzVuDSjP25QhfJDaO9GD50CIYji_aw9c): Berkeley CS186 Class

## Notes

1. Participants send heartbeats periodically. This could be done using `hashicorp/memberlist` (ie SWIM gossip protocol)
2. If the coordinator detects the failure, then it can spin up a new Participant that can read from the durable log (assuming the log is persisted) to create a clone.
3. When the old participant comes back online, the coordinator asks to recycle.
4. For coordinator failure: 3PC, Paxos Commit.
5. Distributed Dead Lock Detection: Periodically each of the Participants sends its wait-for graph to a designated deadlock master node. Then create a union and create a global wait-for graph to detect global deadlock.
6. Also read about 2PL: Growing Phase, Shrinking Phase.[link](https://www.tutorialspoint.com/types-of-two-phase-locking-strict-rigorous-and-conservative-in-dbms)
7. For locking, each participant has a lock table on their own node/machine. For Table level locks, you can have a central node, to track locks on the whole table/database. In MatrixOrigin, we add locks to `mo_tables` or `mo_locks` table.
