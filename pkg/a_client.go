package pkg

import "log"

func (c *MasterClient) Put(key string, value string) (err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply int
	err = c.call("Master.Put", &PutArgs{key, value}, &reply)
	if err != nil {
		log.Println("MasterClient.Put:", err)
		return
	}

	return
}
