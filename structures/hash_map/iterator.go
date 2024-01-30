package hash_map

import (
	"bytes"
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

type RangeIterator struct {
	Iterator
	startKey []byte
	endKey   []byte
}

type PrefixIterator struct {
	Iterator
	prefix []byte
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

func (hm *HashMap) NewRangeIterator(startKey []byte, endKey []byte) (util.Iterator, error) {
	iter, err := hm.NewIterator()
	if err != nil {
		return nil, err
	}

	for bytes.Compare(iter.Value().Key, startKey) < 0 {
		if !iter.Next() {
			return nil, errors.New("error: could not find startKey")
		}
	}

	return &RangeIterator{
		Iterator: *iter.(*Iterator),
		startKey: startKey,
		endKey:   endKey,
	}, nil
}

func (iter *RangeIterator) Next() bool {
	if !iter.Iterator.Next() {
		return false
	}
	if bytes.Compare(iter.Value().Key, iter.endKey) > 0 {
		return false
	}
	return true
}

func (iter *RangeIterator) Value() *model.Record {
	return iter.Iterator.Value()
}

func (hm *HashMap) NewPrefixIterator(prefix []byte) (util.Iterator, error) {
	iter, err := hm.NewIterator()
	if err != nil {
		return nil, err
	}

	for !bytes.HasPrefix(iter.Value().Key, prefix) {
		if !iter.Next() {
			return nil, errors.New("error: could not find prefix")
		}
	}

	return &PrefixIterator{
		Iterator: *iter.(*Iterator),
		prefix:   prefix,
	}, nil
}

func (iter *PrefixIterator) Next() bool {
	if !iter.Iterator.Next() {
		return false
	}
	for !bytes.HasPrefix(iter.Value().Key, iter.prefix) {
		return false
	}
	return true
}

func (iter *PrefixIterator) Value() *model.Record {
	return iter.Iterator.Value()
}
