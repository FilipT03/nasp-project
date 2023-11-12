package hash_map

import (
	"errors"
	"nasp-project/util"
)

type HashMap struct {
	data     map[string]*util.DataRecord
	capacity uint32
}

func NewHashMap(capacity uint32) *HashMap {
	return &HashMap{data: make(map[string]*util.DataRecord), capacity: capacity}
}

func (hm *HashMap) Add(record *util.DataRecord) error {
	if int(hm.capacity) == len(hm.data) {
		return errors.New("error: hashmap is full")
	}
	hm.data[string(record.Key)] = record
	return nil
}

func (hm *HashMap) Delete(key []byte) error {
	record, err := hm.Get(key)
	if err != nil {
		return err
	}
	record.Tombstone = true
	return nil
}

func (hm *HashMap) Get(key []byte) (*util.DataRecord, error) {
	if _, ok := hm.data[string(key)]; ok {
		return nil, nil
	} else {
		return nil, errors.New("error: key '" + string(key) + "' not found in Hash Map")
	}
}

func (hm *HashMap) Flush() []*util.DataRecord {
	return nil
}
