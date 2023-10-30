package bloom_filter

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// getKeys returns a slice of n keys starting from offset.
func getKeys(n, offset uint) [][]byte {
	keys := make([][]byte, n)
	for i := uint(0); i < n; i++ {
		keys[i] = make([]byte, 4)
		binary.LittleEndian.PutUint32(keys[i], uint32(i+offset))
	}
	return keys
}

func TestNewBloomFilterWithSize(t *testing.T) {
	bf := NewBloomFilterWithSize(100, 3)
	if bf == nil {
		t.Error("Expected a new bloom filter.")
	}
	if bf.m != 100 {
		t.Error("Expected a size of 100, got", bf.m)
	}
	if bf.k != 3 {
		t.Error("Expected a k of 3, got", bf.k)
	}
	if len(bf.hashes) != 3 {
		t.Error("Expected 3 hashes, got", len(bf.hashes))
	}
}

func TestNewBloomFilter(t *testing.T) {
	bf := NewBloomFilter(1000, 0.1)
	if bf == nil {
		t.Error("Expected a new bloom filter.")
	}
	if bf.m != 4793 {
		t.Error("Expected a size of 4793, got", bf.m)
	}
	if bf.k != 4 {
		t.Error("Expected a k of 4, got", bf.k)
	}
	if len(bf.hashes) != 4 {
		t.Error("Expected 4 hashes, got", len(bf.hashes))
	}
}

func TestAdd(t *testing.T) {
	bf := NewBloomFilter(1000, 0.1)
	for _, key := range getKeys(1000, 0) {
		bf.Add(key)
	}
	if bf.size != 1000 {
		t.Error("Expected size of 1000, got", bf.size)
	}
}

func TestHasKey(t *testing.T) {
	var bfSize uint = 1000
	bf := NewBloomFilter(bfSize, 0.01)

	for _, key := range getKeys(bfSize, 0) {
		bf.Add(key)
	}
	for _, key := range getKeys(bfSize, 0) {
		if !bf.HasKey(key) {
			t.Error("Expected key to be present.")
		}
	}

	falsePositives := 0
	for _, key := range getKeys(bfSize, bfSize) {
		if bf.HasKey(key) {
			falsePositives++
		}
	}
	fmt.Println("False positives:", falsePositives)
	if falsePositives > 20 {
		t.Error("Expected around 10 false positives, got", falsePositives)
	}
}

func TestSize(t *testing.T) {
	bf := NewBloomFilter(1000, 0.1)
	for _, key := range getKeys(1000, 0) {
		bf.Add(key)
	}
	if bf.Size() != 1000 {
		t.Error("Expected size of 1000, got", bf.Size())
	}
}

func TestSerialize(t *testing.T) {
	bf := NewBloomFilter(1000, 0.1)
	for _, key := range getKeys(1000, 0) {
		bf.Add(key)
	}
	data := bf.Serialize()
	bf2 := Deserialize(data)
	if bf.m != bf2.m {
		t.Error("Expected m to be the same, got", bf2.m)
	}
	if bf.k != bf2.k {
		t.Error("Expected k to be the same, got", bf2.k)
	}
	if bf.size != bf2.size {
		t.Error("Expected size to be the same, got", bf2.size)
	}
	for i := uint(0); i < bf.m; i++ {
		if bf.bitset.Get(i) != bf2.bitset.Get(i) {
			t.Error("Expected bitset to be the same, got", bf2.bitset.Get(i), "at index", i)
		}
	}
}
