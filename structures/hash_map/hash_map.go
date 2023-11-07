package hash_map

import "errors"

type HashMap struct {
	data     map[string][]byte
	capacity uint32
}

func NewHashMap(capacity uint32) *HashMap {
	return &HashMap{data: make(map[string][]byte), capacity: capacity}
}

func (hm *HashMap) Add(key string, value []byte) error {
	if int(hm.capacity) == len(hm.data) {
		return errors.New("error: hashmap is full")
	}
	hm.data[key] = value
	return nil
}

func (hm *HashMap) Delete(key string) error {
	delete(hm.data, key)
	return nil
}

func (hm *HashMap) Get(key string) ([]byte, error) {
	if value, ok := hm.data[key]; ok {
		return value, nil
	} else {
		return nil, errors.New("error: key '" + key + "' not found in Hash Map")
	}
}
