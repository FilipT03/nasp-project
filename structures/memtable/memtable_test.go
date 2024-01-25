package memtable

import (
	"bytes"
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
		t.Errorf("error: [%s] '5' should be in structure", structure)
	}

	record, _ := mts.Get([]byte("5"))
	if err != nil {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
	if !record.Tombstone {
		t.Errorf("error: [%s] '5' should be logically deleted", structure)
	}
	mts.Clear()
}

func TestLogicalDelete(t *testing.T) {
	testLogicalDelete(t, "SkipList")
	testLogicalDelete(t, "BTree")
	testLogicalDelete(t, "HashMap")
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

func addRange(mts *Memtables) {

	_ = mts.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("3"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 3,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("3"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 5,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 4,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 3,
	})

	_ = mts.Add(&model.Record{
		Key:       []byte("3"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 2,
	})
	_ = mts.Add(&model.Record{
		Key:       []byte("6"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 1,
	})
}

func testValidRangeScan(structure string, t *testing.T) {
	util.GetConfig().Memtable.MaxSize = 3
	util.GetConfig().Memtable.Instances = 5
	util.GetConfig().Memtable.Structure = structure
	mts := CreateMemtables(&util.GetConfig().Memtable)
	addRange(mts)

	records := mts.RangeScan([]byte("2"), []byte("8"))
	sol := []model.Record{
		{
			Key:       []byte("2"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 0,
		},
		{
			Key:       []byte("3"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 5,
		},
		{
			Key:       []byte("4"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 0,
		},
		{
			Key:       []byte("5"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 4,
		},
		{
			Key:       []byte("6"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 1,
		},
		{
			Key:       []byte("8"),
			Value:     nil,
			Tombstone: false,
			Timestamp: 3,
		},
	}

	if len(records) != 6 {
		t.Errorf("error: [%s] records size to be 6, got %d", structure, len(records))
		return
	}

	for i, record := range records {
		if bytes.Compare(record.Key, sol[i].Key) != 0 {
			t.Errorf("error: [%s] keys are not sorted correcly at %d", structure, i)
		}
		if record.Timestamp != sol[i].Timestamp {
			t.Errorf("error: [%s] timestamp is not correct at %d", structure, i)
		}
	}
}

func TestValidRangeScan(t *testing.T) {
	testValidRangeScan("HashMap", t)
	testValidRangeScan("BTree", t)
	testValidRangeScan("SkipList", t)
}

func testInvalidRangeScan(structure string, t *testing.T) {
	util.GetConfig().Memtable.MaxSize = 3
	util.GetConfig().Memtable.Instances = 5
	util.GetConfig().Memtable.Structure = structure
	mts := CreateMemtables(&util.GetConfig().Memtable)
	addRange(mts)

	records := mts.RangeScan([]byte("A"), []byte("F"))

	if len(records) != 0 {
		t.Errorf("error: [%s] records size to be 0, got %d", structure, len(records))
	}
}

func TestInvalidRangeScan(t *testing.T) {
	testInvalidRangeScan("HashMap", t)
	testInvalidRangeScan("BTree", t)
	testInvalidRangeScan("SkipList", t)
}
