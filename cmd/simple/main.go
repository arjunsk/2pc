package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"twopc/pkg"
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
		pkg.RunMaster(*replicaCount)
	case *isReplica:
		log.SetPrefix(fmt.Sprint("R", strconv.Itoa(*replicaNumber), " "))
		pkg.RunReplica(*replicaNumber)
	default:
		flag.Usage()
	}
}
