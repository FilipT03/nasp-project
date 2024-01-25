package hash_map

import (
	"errors"
	"nasp-project/model"
	"nasp-project/structures/iterator"
	"sort"
)

type HashMapIter struct {
	keys     []string
	index    int
	hashMap  *HashMap
	maxIndex int
}

func (hm *HashMap) NewIterator() (iterator.Iterator, error) {
	if len(hm.data) == 0 {
		return nil, errors.New("error: hashmap is empty")
	}
	keys := make([]string, 0)
	for k := range hm.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return &HashMapIter{
		keys:     keys,
		hashMap:  hm,
		index:    0,
		maxIndex: len(hm.data),
	}, nil
}

func (h *HashMapIter) Next() bool {
	if h.index+1 < h.maxIndex {
		h.index += 1
		return true
	}
	return false
}

func (h *HashMapIter) Value() *model.Record {
	value, err := h.hashMap.Get([]byte(h.keys[h.index]))
	if err != nil {
		return nil
	}

	return value
}
