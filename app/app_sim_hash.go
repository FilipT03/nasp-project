package app

import (
	"errors"
	"nasp-project/structures/sim_hash"
	"nasp-project/util"
)

// SHAddFingerprint calculates fingerprint of the given text and stores it in the database with the specified key.
func (kvs *KeyValueStore) SHAddFingerprint(key string, text string) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = util.SimHashPrefix + key
	shFingerprint, err := sim_hash.SimHashText(text)
	if err != nil {
		return err
	}

	return kvs.put(key, shFingerprint.Serialize())
}

// SHDeleteFingerprint deletes a sim hash record with the specified key.
func (kvs *KeyValueStore) SHDeleteFingerprint(key string) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = util.SimHashPrefix + key
	return kvs.delete(key)
}

// SHGetHammingDistance calculates the hamming distance between two sim hash records with the specified keys.
func (kvs *KeyValueStore) SHGetHammingDistance(key1 string, key2 string) (uint8, error) {
	if kvs.rateLimitReached() {
		return 0, errors.New("rate limit reached")
	}
	key1 = util.SimHashPrefix + key1
	key2 = util.SimHashPrefix + key2
	shFingerprint1Bytes, err := kvs.get(key1)
	if err != nil {
		return 0, err
	}
	shFingerprint2Bytes, err := kvs.get(key2)
	if err != nil {
		return 0, err
	}
	if shFingerprint1Bytes == nil || shFingerprint2Bytes == nil {
		return 0, errors.New("no sh with given key")
	}
	fingerprint1 := sim_hash.Deserialize(shFingerprint1Bytes)
	fingerprint2 := sim_hash.Deserialize(shFingerprint2Bytes)
	return fingerprint1.HammingDistanceFrom(fingerprint2), nil
}
