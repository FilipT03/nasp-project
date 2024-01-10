package skip_list

import (
	"bytes"
	"nasp-project/model"
	"nasp-project/util"
	"testing"
)

func TestAddAndRemove(t *testing.T) {
	sl := NewSkipList(100, 20)
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keys {
		err := sl.Add(&model.Record{
			Tombstone: false,
			Key:       []byte(key),
			Value:     nil,
			Timestamp: 0,
		})
		if err != nil {
			t.Error(err)
		}
	}

	if !sl.HasKey("key3") {
		t.Error("key3 not found in the skip list")
	}
	if !sl.HasKey("key5") {
		t.Error("key5 not found in the skip list")
	}

	err := sl.Remove([]byte("key2"))
	if err != nil {
		t.Error(err)
	}
	if sl.HasKey("key2") {
		t.Error("key2 not Removed")
	}
	err = sl.Remove([]byte("key4"))
	if err != nil {
		t.Error(err)
	}
	if sl.HasKey("key4") {
		t.Error("key4 not Removed")
	}
}

func TestMaxSize(t *testing.T) {
	sl := NewSkipList(4, 20)
	keys := []string{"key1", "key2", "key3", "key4"}
	for _, key := range keys {
		err := sl.Add(&model.Record{
			Tombstone: false,
			Key:       []byte(key),
			Value:     nil,
			Timestamp: 0,
		})
		if err != nil {
			t.Error(err)
		}
	}
	err := sl.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("key5"),
		Value:     nil,
		Timestamp: 0,
	})
	if err == nil {
		t.Error("Didn't return error for full skip list")
	}

	err = sl.Remove([]byte("key4"))
	if err != nil {
		t.Error(err)
	}

	err = sl.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("key5"),
		Value:     nil,
		Timestamp: 0,
	})
	if err != nil {
		t.Error("Failed to add new key even though there is enough space")
	}

	err = sl.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("key1"),
		Value:     nil,
		Timestamp: 0,
	})
	if err != nil {
		t.Error("Returned error instead of updating the value")
	}
}

func TestFlush(t *testing.T) {
	sl := NewSkipList(uint32(util.GetConfig().Memtable.MaxSize), uint32(util.GetConfig().Memtable.SkipList.MaxHeight))

	_ = sl.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = sl.Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = sl.Add(&model.Record{
		Key:       []byte("7"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	_ = sl.Add(&model.Record{
		Key:       []byte("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = sl.Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = sl.Add(&model.Record{
		Key:       []byte("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	records := sl.Flush()
	sol := [][]byte{[]byte("1"), []byte("2"), []byte("4"), []byte("5"), []byte("7"), []byte("8")}
	for i, record := range records {
		if bytes.Compare(record.Key, sol[i]) != 0 {
			t.Errorf("error: keys are not sorted correcly at %d", i)
		}
	}
}
