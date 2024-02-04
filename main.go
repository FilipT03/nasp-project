package main

import (
	"nasp-project/app"
	"nasp-project/util"
)

func main() {
	config := util.LoadConfig(util.ConfigPath)

	db, err := app.NewKeyValueStore(config)
	if err != nil {
		panic(err)
	}

	app.Start(db)
}
