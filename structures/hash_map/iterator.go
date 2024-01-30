package hash_map

import (
	"errors"
	"nasp-project/model"
	"nasp-project/util"
	"sort"
)

type Iterator struct {
	keys     []string
	index    int
	hashMap  *HashMap
	maxIndex int
}

func (hm *HashMap) NewIterator() (util.Iterator, error) {
	if len(hm.data) == 0 {
		return nil, errors.New("error: hashmap is empty")
	}
	keys := make([]string, 0)
	for k := range hm.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	iter := &Iterator{
		keys:     keys,
		hashMap:  hm,
		index:    0,
		maxIndex: len(hm.data),
	}
	for util.IsInvalidKey(iter) {
		iter.index += 1
	}
	return iter, nil
}

func (h *Iterator) Next() bool {
	h.index += 1
	for util.IsInvalidKey(h) {
		h.index += 1
	}
	return h.index < h.maxIndex
}

func (h *Iterator) Value() *model.Record {
	if h.index < h.maxIndex {
		value, err := h.hashMap.Get([]byte(h.keys[h.index]))
		if err != nil {
			return nil
		}
		return value
	}
	return nil
}
