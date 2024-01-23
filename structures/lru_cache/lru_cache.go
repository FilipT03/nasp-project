package lru_cache

import (
	"container/list"
	"fmt"
	"nasp-project/model"
)

type LRUCache struct {
	capacity uint64
	cache    map[string]*list.Element
	list     *list.List
}

func NewLRUCache(capacity uint64) LRUCache {
	lruCache := LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		list:     list.New(),
	}

	return lruCache
}

// Get returns *Data for specified key, or nil if absent
func (LRU *LRUCache) Get(key string) *model.Record {
	element := LRU.get(key)
	if element != nil {
		return element.Value.(*model.Record)
	}
	return nil
}

// Used for my Put function to get list element
func (LRU *LRUCache) get(key string) *list.Element {

	value, ok := LRU.cache[key]
	if ok {
		LRU.list.MoveToFront(value)
		return value
	}
	return nil

}

// Put adds or updates the value for specified key
func (LRU *LRUCache) Put(record *model.Record) {
	key := string(record.Key)
	findElement := LRU.get(key)

	//if the record already exists
	if findElement != nil {
		//deleting from the map
		delete(LRU.cache, key)
		//deleting from the list
		LRU.list.Remove(findElement)
		LRU.list.PushFront(record)
		LRU.cache[key] = LRU.list.Front()
		return

	}
	//else create new record
	//case cache is full, and we delete the last(LRU) record
	if uint64(LRU.list.Len()) == LRU.capacity {
		//deleting from the map
		delete(LRU.cache, string(LRU.list.Back().Value.(*model.Record).Key))
		//deleting from the list
		LRU.list.Remove(LRU.list.Back())
	}

	LRU.list.PushFront(record)
	LRU.cache[key] = LRU.list.Front()
	return

}

// Print prints the current cache state.
func (LRU *LRUCache) Print() {
	i := 0
	node := LRU.list.Front()
	for node != nil {

		fmt.Println(i, node.Value.(*model.Record).Key, node.Value.(*model.Record).Value)
		node = node.Next()
		i++
	}

}
