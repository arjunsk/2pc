package main

import (
	"twopc/pkg/client"
	"twopc/pkg/common"
)

func main() {

	c := client.NewMasterClient(common.MasterPort)

	// test put get alone
	err := c.Put("alice", "john")
	if err != nil {
		panic(err)
	}
	println("inserted")

	val, err := c.Get("alice")
	if err != nil {
		panic(err)
	}
	println("get val", *val)

	//// test put get del
	//err = c.Put("db", "cooper")
	//if err != nil {
	//	panic(err)
	//}
	//println("inserted")
	//
	//val, err = c.Get("db")
	//if err != nil {
	//	panic(err)
	//}
	//println("get val", *val)
	//
	//err = c.Del("db")
	//if err != nil {
	//	panic(err)
	//}
	//println("deleted")
	//
	//val, err = c.Get("db")
	//if err == nil {
	//	panic("should have errored")
	//}
	//println("get val nil: ", val == nil)
}
