package lru_cache

import (
	"testing"
)

func TestLRUCache(t *testing.T) {
	capacity := uint64(3) // Adjust capacity as needed
	lruCache := NewLRUCache(capacity)

	// Test Put and Get
	lruCache.Put(1, []byte("one"))
	lruCache.Put(2, []byte("two"))
	lruCache.Put(3, []byte("three"))
	lruCache.Put(4, []byte("four")) // This should trigger eviction of "one"

	element := lruCache.Get(1)
	if element != nil {
		t.Errorf("Expected key 1 to be evicted, but it's still in the cache.")
	}

	// Test Print
	lruCache.Print() // Print the current cache state

}

func TestLRUCacheGetNonExistentKey(t *testing.T) {
	capacity := uint64(3)
	lruCache := NewLRUCache(capacity)

	// Test Get for a key that does not exist in the cache
	element := lruCache.Get(1)
	if element != nil {
		t.Errorf("Expected Get for a non-existent key to return nil, but it didn't.")
	}

}
