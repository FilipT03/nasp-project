package lru_cache

import (
	"nasp-project/model"
	"testing"
	"time"
)

func TestLRUCache(t *testing.T) {
	capacity := uint64(3) // Adjust capacity as needed
	lruCache := NewLRUCache(capacity)

	// Test Put and Get
	key1 := []byte("one")
	value1 := []byte("value1")
	rec1 := model.Record{
		Key:       key1,
		Value:     value1,
		Tombstone: false,
		Timestamp: uint64(time.Now().Unix()),
	}
	lruCache.Put(&rec1)

	key2 := []byte("two")
	value2 := []byte("value2")
	rec2 := model.Record{
		Key:       key2,
		Value:     value2,
		Tombstone: false,
		Timestamp: uint64(time.Now().Unix()),
	}
	lruCache.Put(&rec2)

	key3 := []byte("three")
	value3 := []byte("value3")
	rec3 := model.Record{
		Key:       key3,
		Value:     value3,
		Tombstone: false,
		Timestamp: uint64(time.Now().Unix()),
	}
	lruCache.Put(&rec3)

	key4 := []byte("four")
	value4 := []byte("value4")
	rec4 := model.Record{
		Key:       key4,
		Value:     value4,
		Tombstone: false,
		Timestamp: uint64(time.Now().Unix()),
	}
	lruCache.Put(&rec4) // This should trigger eviction of rec1

	element := lruCache.Get(string(key1))
	if element != nil {
		t.Errorf("Expected key %s to be evicted, but it's still in the cache.", key1)
	}

	// Test Print
	lruCache.Print() // Print the current cache state
}

func TestLRUCacheGetNonExistentKey(t *testing.T) {
	capacity := uint64(3)
	lruCache := NewLRUCache(capacity)

	// Test Get for a key that does not exist in the cache
	key := "nonexistent_key"
	element := lruCache.Get(key)
	if element != nil {
		t.Errorf("Expected Get for a non-existent key to return nil, but it didn't.")
	}
}
