package bitset

import (
	"math/rand"
	"testing"
)

func TestBitset(t *testing.T) {
	b := NewBitset(1000)

	for i := uint(0); i < 1000; i++ {
		if b.Get(i) != 0 {
			t.Errorf("Expected bit %d to be unset", i)
		}
	}

	for i := uint(0); i < 1000; i++ {
		b.Set(i, 1)
	}
	for i := uint(0); i < 1000; i++ {
		if b.Get(i) != 1 {
			t.Errorf("Expected bit %d to be set", i)
		}
	}

	for i := uint(0); i < 1000; i++ {
		b.Set(i, 0)
	}
	for i := uint(0); i < 1000; i++ {
		if b.Get(i) != 0 {
			t.Errorf("Expected bit %d to be unset", i)
		}
	}

	idx := rand.Uint64() % 1000
	b.Set(uint(idx), 1)
	if b.Get(uint(idx)) != 1 {
		t.Errorf("Expected bit %d to be set", idx)
	}

	b.Set(uint(idx), 0)
	if b.Get(uint(idx)) != 0 {
		t.Errorf("Expected bit %d to be unset", idx)
	}
}

func TestSerialize(t *testing.T) {
	b := NewBitset(1000)
	for i := uint(0); i < 1000; i += 2 {
		b.Set(i, 1)
	}
	data := b.Serialize()
	b2 := Deserialize(data)
	if b.n != b2.n {
		t.Error("Expected n to be the same, got", b2.n)
	}
	for i := uint(0); i < b.n; i++ {
		if b.Get(i) != b2.Get(i) {
			t.Errorf("Expected bit %d to be %d, got %d", i, b.Get(i), b2.Get(i))
		}
	}
}
