package memtable

import (
	"bytes"
	"log"
	"nasp-project/model"
	"nasp-project/util"
	"testing"
)

func add(mt *Memtable) {
	mt.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	mt.Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	mt.Add(&model.Record{
		Key:       []byte("7"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	mt.Add(&model.Record{
		Key:       []byte("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	mt.Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	mt.Add(&model.Record{
		Key:       []byte("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
}

func testLogicalDelete(t *testing.T, structure string) {
	util.GetConfig().MemTable.Structure = structure
	mt := NewMemtable(&util.GetConfig().MemTable)
	add(mt)
	err := mt.Delete([]byte("5"))
	if err != nil {
		t.Errorf("error: [%s] '5' should be in SkipList", structure)
	}

	record, _ := mt.Get([]byte("5"))
	if !record.Tombstone {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
}

//func testFullSize(t *testing.T, structure string) {
//	util.GetConfig().MemTable.MaxSize = 6
//	util.GetConfig().MemTable.Structure = structure
//	mt := NewMemtable(&util.GetConfig().MemTable)
//	add(mt)
//	err := mt.Add(&model.Record{
//		Key:       []byte("69"),
//		Value:     nil,
//		Tombstone: false,
//		Timestamp: 0,
//	})
//	if err == nil {
//		t.Errorf("error: %s should be full", structure)
//	}
//}

func testFlush(t *testing.T, structure string) {
	util.GetConfig().MemTable.Structure = structure

	mt := NewMemtable(&util.GetConfig().MemTable)
	add(mt)
	records := mt.structure.Flush()
	sol := [][]byte{[]byte("1"), []byte("2"), []byte("4"), []byte("5"), []byte("7"), []byte("8")}
	for i, record := range records {
		log.Printf("[%s] : %d - %s ", structure, i, string(record.Key))
		if bytes.Compare(record.Key, sol[i]) != 0 {
			t.Errorf("error: [%s] keys are not sorted correcly at %d", structure, i)
		}
	}
}

//func TestFullSize(t *testing.T) {
//	testFullSize(t, "SkipList")
//	testFullSize(t, "BTree")
//	testFullSize(t, "HashMap")
//}

func TestLogicalDelete(t *testing.T) {
	testLogicalDelete(t, "SkipList")
	testLogicalDelete(t, "BTree")
	testLogicalDelete(t, "HashMap")
}

func TestFlush(t *testing.T) {
	testFlush(t, "BTree")
	testFlush(t, "SkipList")
}
