package app

import (
	"errors"
	"nasp-project/structures/hyperloglog"
	"nasp-project/util"
)

// NewHLL creates a new hyperloglog record with specified key.
// Returns an error if the write fails or the rate limit is reached.
func (kvs *KeyValueStore) NewHLL(key string, p uint32) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = util.HyperLogLogPrefix + key
	hll := hyperloglog.NewHyperLogLog(p)
	return kvs.put(key, hll.Serialize())
}

// DeleteHLL deletes a hyperloglog record with the specified key.
// Returns an error if the write fails or the rate limit is reached.
func (kvs *KeyValueStore) DeleteHLL(key string) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = util.HyperLogLogPrefix + key
	return kvs.delete(key)
}

// HLLAdd performs Add(val) operation on a hyperloglog record with the specified key.
// Returns an error if no hyperloglog record with the given key exists.
func (kvs *KeyValueStore) HLLAdd(key string, val []byte) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = util.HyperLogLogPrefix + key
	hllBytes, err := kvs.get(key)
	if err != nil {
		return err
	}
	if hllBytes == nil {
		return errors.New("no hll with given key")
	}
	hll := hyperloglog.Deserialize(hllBytes)
	hll.Add(val)
	return kvs.put(key, hll.Serialize())
}

// HLLEstimate performs Add(val) operation on a hyperloglog record with the specified key.
// Returns an error if no hyperloglog record with the given key exists.
func (kvs *KeyValueStore) HLLEstimate(key string) (float64, error) {
	if kvs.rateLimitReached() {
		return -1, errors.New("rate limit reached")
	}
	key = util.HyperLogLogPrefix + key
	hllBytes, err := kvs.get(key)
	if err != nil {
		return -1, err
	}
	if hllBytes == nil {
		return -1, errors.New("no hll with given key")
	}
	hll := hyperloglog.Deserialize(hllBytes)
	estimation := hll.Estimate()
	return estimation, nil
}
