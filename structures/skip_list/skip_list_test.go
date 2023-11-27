package skip_list

import (
	"nasp-project/model"
	"testing"
)

func TestAddAndDelete(t *testing.T) {
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

	err := sl.Delete([]byte("key2"))
	if err != nil {
		t.Error(err)
	}
	if sl.HasKey("key2") {
		t.Error("key2 not deleted")
	}
	err = sl.Delete([]byte("key4"))
	if err != nil {
		t.Error(err)
	}
	if sl.HasKey("key4") {
		t.Error("key4 not deleted")
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

	err = sl.Delete([]byte("key4"))
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
