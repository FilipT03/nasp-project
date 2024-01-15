package app

import (
	"errors"
	"nasp-project/structures/bloom_filter"
)

const BloomFilterPrefix = "BF_"

func (kvs *KeyValueStore) NewBF(key string, n uint, p float64) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = BloomFilterPrefix + key
	bf := bloom_filter.NewBloomFilter(n, p)
	return kvs.put(key, bf.Serialize())
}

func (kvs *KeyValueStore) DeleteBF(key string) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = BloomFilterPrefix + key
	return kvs.delete(key)
}

func (kvs *KeyValueStore) BFAdd(key string, val []byte) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = BloomFilterPrefix + key

	bfBytes, err := kvs.get(key)
	if err != nil {
		return err
	}
	if bfBytes == nil {
		return errors.New("no bf with given key")
	}

	bf := bloom_filter.Deserialize(bfBytes)
	bf.Add(val)

	return kvs.put(key, bf.Serialize())
}

func (kvs *KeyValueStore) BFHasKey(key string, val []byte) (bool, error) {
	if kvs.rateLimitReached() {
		return false, errors.New("rate limit reached")
	}
	key = BloomFilterPrefix + key

	bfBytes, err := kvs.get(key)
	if err != nil {
		return false, err
	}
	if bfBytes == nil {
		return false, errors.New("no bf with given key")
	}

	bf := bloom_filter.Deserialize(bfBytes)
	ans := bf.HasKey(val)

	return ans, kvs.put(key, bf.Serialize())
}
