package memtable

import (
	"nasp-project/model"
	"nasp-project/util"
	"testing"
)

func add() {
	Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	Add(&model.Record{
		Key:       []byte("7"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	Add(&model.Record{
		Key:       []byte("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	Add(&model.Record{
		Key:       []byte("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
}

func testLogicalDelete(t *testing.T, structure string) {
	util.GetConfig().Memtable.Structure = structure
	CreateMemtables(&util.GetConfig().Memtable)
	add()
	//err := memtable.Delete([]byte("5"))
	//if err != nil {
	//	t.Errorf("error: [%s] '5' should be in SkipList", structure)
	//}
	//
	//record, _ := mt.Get([]byte("5"))
	//if !record.Tombstone {
	//	t.Errorf("error: [%s] '5' should be logically deleted", structure)
	//}
}

func testFlush(t *testing.T, structure string) {
	util.GetConfig().Memtable.Structure = structure
	util.GetConfig().Memtable.Instances = 1
	CreateMemtables(&util.GetConfig().Memtable)
	add()
	//records := memtable.structure.Flush()
	//sol := [][]byte{[]byte("1"), []byte("2"), []byte("4"), []byte("5"), []byte("7"), []byte("8")}
	//for i, record := range records {
	//	log.Printf("[%s] : %d - %s ", structure, i, string(record.Key))
	//	if bytes.Compare(record.Key, sol[i]) != 0 {
	//		t.Errorf("error: [%s] keys are not sorted correcly at %d", structure, i)
	//	}
	//}
}

func TestLogicalDelete(t *testing.T) {
	testLogicalDelete(t, "SkipList")
	testLogicalDelete(t, "BTree")
	testLogicalDelete(t, "HashMap")
}

func TestFlush(t *testing.T) {
	testFlush(t, "BTree")
	testFlush(t, "SkipList")
	testFlush(t, "HashMap")
}
