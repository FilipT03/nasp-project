package hash

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"math/rand"
	"time"
)

// SeededHash is a hash function that uses a seed value.
type SeededHash struct {
	Seed []byte // 8 bytes
	hash hash.Hash64
}

// NewSeededHash returns a new SeededHash instance.
func NewSeededHash(seed uint64) SeededHash {
	seedBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(seedBytes, seed)
	return SeededHash{
		seedBytes,
		fnv.New64a(),
	}
}

// Hash returns a 64-bit hash of the key.
func (h *SeededHash) Hash(key []byte) uint64 {
	h.hash.Reset()
	_, _ = h.hash.Write(h.Seed)
	_, _ = h.hash.Write(key)
	return h.hash.Sum64()
}

// CreateHashes returns a slice of n SeededHash instances.
func CreateHashes(n uint) []SeededHash {
	ts := uint64(time.Now().Unix()) + rand.Uint64()
	var hashes []SeededHash
	for i := uint(0); i < n; i++ {
		hashes = append(hashes, NewSeededHash(ts+uint64(i)))
	}
	return hashes
}
