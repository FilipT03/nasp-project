package memtable

import (
	"nasp-project/model"
	"nasp-project/util"
	"testing"
)

func add(mt *Memtable) {
	_ = mt.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mt.Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mt.Add(&model.Record{
		Key:       []byte("7"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
}

func TestAddFullSizeSkipList(t *testing.T) {
	util.GetConfig().MemTable.MaxSize = 3
	util.GetConfig().MemTable.Structure = "SkipList"
	mt := NewMemtable(&util.GetConfig().MemTable)
	add(mt)
	err := mt.Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err == nil {
		t.Error("Memtable should be full")
	}
}

func TestAddFullSizeBTree(t *testing.T) {
	util.GetConfig().MemTable.MaxSize = 3
	util.GetConfig().MemTable.Structure = "BTree"
	mt := NewMemtable(&util.GetConfig().MemTable)
	add(mt)
	err := mt.Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err == nil {
		t.Error("Memtable should be full")
	}
}

func TestAddFullSizeHashMap(t *testing.T) {
	util.GetConfig().MemTable.MaxSize = 3
	util.GetConfig().MemTable.Structure = "HashMap"
	mt := NewMemtable(&util.GetConfig().MemTable)
	add(mt)
	err := mt.Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	if err == nil {
		t.Error("Memtable should be full")
	}
}
