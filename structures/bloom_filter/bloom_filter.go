package bloom_filter

import (
	"math"
	"nasp-project/structures/bitset"
	"nasp-project/structures/hash"
)

type BloomFilter struct {
	m      uint // size of bitset
	bitset *bitset.Bitset
	k      uint // number of hash functions
	hashes []hash.SeededHash
	size   uint // number of elements added
}

// NewBloomFilterWithSize returns a new BloomFilter instance with bitset of size m and k hash functions.
func NewBloomFilterWithSize(m, k uint) *BloomFilter {
	return &BloomFilter{
		m,
		bitset.NewBitset(m),
		k, // arbitrary value (modified if OptionSize is provided)
		hash.CreateHashes(k),
		0,
	}
}

// NewBloomFilter returns a new BloomFilter with size of the bitset and number of hash functions calculated
// based on the expected number of elements (n) and desired false-positive probability (p).
func NewBloomFilter(n uint, p float64) *BloomFilter {
	m := uint(math.Ceil(float64(n) * math.Abs(math.Log(p)) / math.Pow(math.Log(2), float64(2))))
	k := uint(math.Ceil((float64(m) / float64(n)) * math.Log(2)))
	return NewBloomFilterWithSize(m, k)
}

// getIdx Returns the slice of indices in bitset for a given key
func (bf *BloomFilter) getIdx(key []byte) []uint {
	var res []uint
	for _, h := range bf.hashes {
		idx := uint(h.Hash(key) % uint64(bf.m))
		res = append(res, idx)
	}
	return res
}

// Add Adds a key to the set.
func (bf *BloomFilter) Add(key []byte) {
	for _, idx := range bf.getIdx(key) {
		bf.bitset.Set(idx, 1)
	}
	bf.size++
}

// HasKey Returns true if the key was added to the set, false otherwise.
// False-positives are possible, false-negatives are not.
func (bf *BloomFilter) HasKey(key []byte) bool {
	for _, idx := range bf.getIdx(key) {
		if bf.bitset.Get(idx) == 0 {
			return false
		}
	}
	return true
}

// Size Returns the number of elements added to the set.
func (bf *BloomFilter) Size() uint {
	return bf.size
}
