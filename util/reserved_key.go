package util

import "bytes"

// IsReservedKey returns true if the provided key is reserved for internal workings of the key-value storage engine
// and thus must not be used as a regular key.
func IsReservedKey(key []byte) bool {
	reservedKeys := [][]byte{
		[]byte(RateLimiterKey),
	}
	reservedPrefixes := [][]byte{
		[]byte(BloomFilterPrefix),
		[]byte(CountMinSketchPrefix),
		[]byte(HyperLogLogPrefix),
		[]byte(SimHashPrefix),
	}
	for _, rKey := range reservedKeys {
		if bytes.Equal(key, rKey) {
			return true
		}
	}
	for _, prefix := range reservedPrefixes {
		if bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
