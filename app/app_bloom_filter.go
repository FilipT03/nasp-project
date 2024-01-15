package app

import (
	"errors"
	"nasp-project/structures/bloom_filter"
)

const BloomFilterPrefix = "BF_"

// NewBF creates a new bloom filter record with specified key.
// Size of the bitset and number of hash functions calculated based on
// the expected number of elements (n) and desired false-positive probability (p).
// Returns an error if the write fails or the rate limit is reached.
func (kvs *KeyValueStore) NewBF(key string, n uint, p float64) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = BloomFilterPrefix + key
	bf := bloom_filter.NewBloomFilter(n, p)
	return kvs.put(key, bf.Serialize())
}

// DeleteBF deletes a bloom filter record with the specified key.
// Returns an error if the write fails or the rate limit is reached.
func (kvs *KeyValueStore) DeleteBF(key string) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = BloomFilterPrefix + key
	return kvs.delete(key)
}

// BFAdd performs Add(val) operation on a bloom filter record with the specified key.
// Returns an error if no bloom filter record with the given key exists.
// Returns an error if the write fails or the rate limit is reached.
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

// BFHasKey performs HasKey(val) operation on a bloom filter record with the specified key.
// Returns an error if no bloom filter record with the given key exists.
// Returns an error if the read fails or the rate limit is reached.
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

	return bf.HasKey(val), nil
}
