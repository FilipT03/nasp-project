package main

import (
	"fmt"
	write_ahead_log "nasp-project/structures/write-ahead_log"
)

func main() {
	//config := util.GetConfig()

	//wal, err := write_ahead_log.NewWAL(uint32(config.WAL.BufferSize), uint64(config.WAL.SegmentSize))
	wal, err := write_ahead_log.NewWAL(8, 128)
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Println(wal.LatestFileName)
	wal.Test()

	return

	/*sl := skip_list.NewSkipList(100, 1)
	sl.Add("1", make([]byte, 0))
	sl.Add("2", make([]byte, 0))
	sl.Add("3", make([]byte, 0))
	sl.Add("4", make([]byte, 0))
	sl.Add("5", make([]byte, 0))

	sl.Print()
	fmt.Println("----------------")

	sl.Delete("4")

	sl.Print()
	fmt.Println("----------------")

	err := sl.Update("2", make([]byte, 1))
	if err != nil {
		panic(err)
	}

	sl.Print()
	fmt.Println("----------------")

	fmt.Println("Hello, world!")*/
}
