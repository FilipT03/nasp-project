package main

import (
	"fmt"
	"nasp-project/app"
	"nasp-project/util"
)

const ConfigPath = "config.yaml"

func main() {
	config := util.LoadConfig(ConfigPath)

	db, err := app.NewKeyValueStore(config)
	if err != nil {
		panic(err)
	}

	//for i := 0; i < 1050; i++ {
	//	key := fmt.Sprintf("key%d", i)
	key := "hey"

	err = db.Put(key, []byte{1, 2, 3})
	if err != nil {
		panic(err)
	}

	val, err := db.Get(key)
	if err != nil {
		panic(err)
	}
	if val == nil {
		fmt.Println("empty")
	} else {
		fmt.Println(val)
	}
	//}
}
