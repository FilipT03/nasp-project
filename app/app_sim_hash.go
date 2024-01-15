package app

import (
	"errors"
	"nasp-project/structures/sim_hash"
)

const SimHashPrefix = "SH_"

// NewSH calculates fingerprint of the given text and stores it in the database with the specified key.
func (kvs *KeyValueStore) NewSH(key string, text string) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = SimHashPrefix + key
	shFingerprint, err := sim_hash.SimHashText(text)
	if err != nil {
		return err
	}

	return kvs.put(key, shFingerprint.Serialize())
}

// DeleteSH deletes a sim hash record with the specified key.
func (kvs *KeyValueStore) DeleteSH(key string) error {
	if kvs.rateLimitReached() {
		return errors.New("rate limit reached")
	}
	key = SimHashPrefix + key
	return kvs.delete(key)
}

// GetHammingDistance calculates the hamming distance between two sim hash records with the specified keys.
func (kvs *KeyValueStore) GetHammingDistance(key1 string, key2 string) (uint8, error) {
	if kvs.rateLimitReached() {
		return 0, errors.New("rate limit reached")
	}
	key1 = SimHashPrefix + key1
	key2 = SimHashPrefix + key2
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
