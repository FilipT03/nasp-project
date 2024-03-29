package app

import (
	"errors"
	count_min_sketch "nasp-project/structures/count-min-sketch"
	"nasp-project/util"
)

// NewCMS creates a new count-min sketch record with specified key.
// Returns an error if the write fails or the rate limit is reached.
func (kvs *KeyValueStore) NewCMS(key string, epsilon float64, delta float64) error {
	if block, err := kvs.rateLimitReached(); block {
		if err != nil {
			return err
		}
		return errors.New("rate limit reached")
	}
	key = util.CountMinSketchPrefix + key
	cms := count_min_sketch.NewCMS(epsilon, delta)
	return kvs.put(key, cms.Serialize())
}

// DeleteCMS deletes a count-min sketch record with the specified key.
// Returns an error if the write fails or the rate limit is reached.
func (kvs *KeyValueStore) DeleteCMS(key string) error {
	if block, err := kvs.rateLimitReached(); block {
		if err != nil {
			return err
		}
		return errors.New("rate limit reached")
	}
	key = util.CountMinSketchPrefix + key
	return kvs.delete(key)
}

// CMSAdd performs Add(val) operation on a count-min sketch record with the specified key.
// Returns an error if no count-min sketch record with the given key exists.
func (kvs *KeyValueStore) CMSAdd(key string, val []byte) error {
	if block, err := kvs.rateLimitReached(); block {
		if err != nil {
			return err
		}
		return errors.New("rate limit reached")
	}
	key = util.CountMinSketchPrefix + key
	CMSBytes, err := kvs.get(key)
	if err != nil {
		return err
	}
	if CMSBytes == nil {
		return errors.New("no cms with the given key")
	}
	cms := count_min_sketch.Deserialize(CMSBytes)
	cms.Add(val)

	return kvs.put(key, cms.Serialize())
}

// CMSGet performs Estimate(val) operation on a count-min sketch record with the specified key.
// Returns an error if no count-min sketch record with the given key exists.
func (kvs *KeyValueStore) CMSGet(key string, val []byte) (int, error) {
	if block, err := kvs.rateLimitReached(); block {
		if err != nil {
			return -1, err
		}
		return -1, errors.New("rate limit reached")
	}
	key = util.CountMinSketchPrefix + key
	CMSBytes, err := kvs.get(key)
	if err != nil {
		return -1, err
	}
	if CMSBytes == nil {
		return -1, errors.New("no cms with given key")
	}
	cms := count_min_sketch.Deserialize(CMSBytes)
	return cms.Get(val), nil
}
