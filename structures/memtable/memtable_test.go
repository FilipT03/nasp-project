package memtable

import (
	"nasp-project/model"
	"nasp-project/util"
	"testing"
)

func add(mts *Memtables) {
	err := mts.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mts.Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mts.Add(&model.Record{
		Key:       []byte("7"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mts.Add(&model.Record{
		Key:       []byte("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mts.Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
	err = mts.Add(&model.Record{
		Key:       []byte("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err != nil {
		panic(err)
	}
}

func testLogicalDelete(t *testing.T, structure string) {
	util.GetConfig().Memtable.Structure = structure
	mts := CreateMemtables(&util.GetConfig().Memtable)
	add(mts)
	err := mts.Delete([]byte("5"))
	if err != nil {
		t.Errorf("error: [%s] '5' should be in SkipList", structure)
	}

	record, _ := mts.Get([]byte("5"))
	if err != nil {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
	if !record.Tombstone {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
}

func TestLogicalDelete(t *testing.T) {
	mts := CreateMemtables(&util.GetConfig().Memtable)
	testLogicalDelete(t, "SkipList")
	testLogicalDelete(t, "BTree")
	testLogicalDelete(t, "HashMap")
	mts.Clear()
}

func TestTableSwitch(t *testing.T) {
	util.GetConfig().Memtable.Instances = 4
	util.GetConfig().Memtable.MaxSize = 3
	mts := CreateMemtables(&util.GetConfig().Memtable)
	add(mts)
	err := mts.Add(&model.Record{
		Key:       []byte("a"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	if err != nil {
		t.Errorf("error: [%s]", err.Error())
	}

	if mts.currentIndex != 2 {
		t.Errorf("error: expected current table to be %d, but got %d", 2, mts.currentIndex)
	}
}
