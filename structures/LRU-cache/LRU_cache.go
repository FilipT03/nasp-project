package LRU_cache

import (
	"container/list"
	"fmt"
)

type Data struct {
	key   uint64
	value []byte
}

type LRUCache struct {
	capacity uint64
	cache    map[uint64]*list.Element
	list     *list.List
}

func NewLRUCache(capacity uint64) LRUCache {
	lruCache := LRUCache{
		capacity: capacity,
		cache:    make(map[uint64]*list.Element),
		list:     list.New(),
	}

	return lruCache
}

func (LRU *LRUCache) Get(key uint64) *list.Element {

	value, ok := LRU.cache[key]
	if ok {
		//TODO update to most recent
		return value
	}
	return nil

}
func (LRU *LRUCache) Put(key uint64, value []byte) {
	data := Data{key: key, value: value}
	findElement := LRU.Get(key)

	//if the record already exists
	if findElement != nil {
		//deleting from the map
		delete(LRU.cache, findElement.Value.(Data).key)
		//deleting from the list
		LRU.list.Remove(findElement)
		LRU.list.PushFront(data)
		LRU.cache[key] = LRU.list.Front()
		return

	}
	//else create new record
	//case cache is full, and we delete the last(LRU) record
	if uint64(LRU.list.Len()) == LRU.capacity {
		//deleting from the map
		delete(LRU.cache, LRU.list.Back().Value.(Data).key)
		//deleting from the list
		LRU.list.Remove(LRU.list.Back())
	}

	LRU.list.PushFront(data)
	LRU.cache[key] = LRU.list.Front()
	return

}
func (LRU *LRUCache) Print() {
	i := 0
	node := LRU.list.Front()
	for node != nil {

		fmt.Println(i, node.Value.(Data).key, node.Value.(Data).value)
		node = node.Next()
		i++
	}

}
