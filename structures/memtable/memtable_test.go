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
	err := Delete([]byte("5"))
	if err != nil {
		t.Errorf("error: [%s] '5' should be in SkipList", structure)
	}

	record, _ := Get([]byte("5"))
	if err != nil {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
	if !record.Tombstone {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
}

func TestLogicalDelete(t *testing.T) {
	testLogicalDelete(t, "SkipList")
	testLogicalDelete(t, "BTree")
	testLogicalDelete(t, "HashMap")
	Clear()
}

func TestTableSwitch(t *testing.T) {
	util.GetConfig().Memtable.Instances = 4
	util.GetConfig().Memtable.MaxSize = 3
	CreateMemtables(&util.GetConfig().Memtable)
	add()
	Add(&model.Record{
		Key:       []byte("a"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	if Memtables.currentIndex != 2 {
		t.Errorf("error: expected current table to be %d, but got %d", 2, Memtables.currentIndex)
	}
}
