package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"twopc/pkg/master"
	"twopc/pkg/replica"
)

func main() {
	isMaster := flag.Bool("master", false, "start the master process")
	replicaCount := flag.Int("replicaCount", 0, "replica count for master")

	isReplica := flag.Bool("replica", false, "start a replica process")
	replicaNumber := flag.Int("replicaIndex", 0, "replica index to run, starting at 0")

	flag.Parse()

	switch {
	case *isMaster:
		log.SetPrefix("M  ")
		master.RunMaster(*replicaCount)
	case *isReplica:
		log.SetPrefix(fmt.Sprint("R", strconv.Itoa(*replicaNumber), " "))
		replica.RunReplica(*replicaNumber)
	default:
		flag.Usage()
	}
}
