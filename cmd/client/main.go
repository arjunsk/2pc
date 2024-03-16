package main

import (
	"twopc/pkg/client"
	"twopc/pkg/common"
)

func main() {

	c := client.NewMasterClient(common.MasterPort)

	err := c.Put("arjun", "student")
	if err != nil {
		panic(err)
	}
	println("inserted")

	val, err := c.Get("arjun")
	if err != nil {
		panic(err)
	}
	println("get val", *val)

	err = c.Del("arjun")
	if err != nil {
		panic(err)
	}
	println("deleted")

	val, err = c.Get("arjun")
	if err == nil {
		panic("should have errored")
	}
	println("get val nil: ", val == nil)
}
