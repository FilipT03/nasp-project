package hash_map

import (
	"errors"
	"fmt"
	"nasp-project/model"
	"sort"
)

type HashMap struct {
	data     map[string]*model.Record
	capacity uint32
}

func NewHashMap(capacity uint32) *HashMap {
	return &HashMap{data: make(map[string]*model.Record), capacity: capacity}
}

func (hm *HashMap) Add(record *model.Record) error {
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

func (hm *HashMap) Get(key []byte) (*model.Record, error) {
	if _, ok := hm.data[string(key)]; ok {
		return hm.data[string(key)], nil
	} else {
		return nil, errors.New("error: key '" + string(key) + "' not found in Hash Map")
	}
}

func (hm *HashMap) Flush() []model.Record {
	keys := make([]string, 0, hm.capacity)
	for k, _ := range hm.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	records := make([]model.Record, hm.capacity)

	for _, k := range keys {
		records = append(records, *hm.data[k])
	}

	hm.data = make(map[string]*model.Record)
	fmt.Println(records)
	return records
}
