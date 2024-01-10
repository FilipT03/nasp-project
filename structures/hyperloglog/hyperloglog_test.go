package hyperloglog

import (
	"math"
	"testing"
)

// TestSerialize tests Serialized and Deserialized.
func TestSerialize(t *testing.T) {
	hll := NewHyperLogLog(5)
	for i := 0; i < 1_000; i++ {
		hll.Add([]byte("Lazar"))
		hll.Add([]byte("Dusan"))
		hll.Add([]byte("Milena"))
		hll.Add([]byte("Svetozar"))
		hll.Add([]byte("Stefan"))
		hll.Add([]byte("Vanja"))
	}
	data := hll.Serialize()
	deserialized := Deserialize(data)

	if hll.m != deserialized.m {
		t.Errorf("Expected m to be the same, got %d", deserialized.m)
	}
	if hll.p != deserialized.p {
		t.Errorf("Expected p to be the same, got %d", deserialized.p)
	}
	h1 := hll.hash.Hash([]byte("Milos"))
	h2 := deserialized.hash.Hash([]byte("Milos"))
	if h1 != h2 {
		t.Errorf("Expected hashed values to be the same, got %d instead of %d", h2, h1)
	}
	for i := uint32(0); i < hll.m; i++ {
		if hll.reg[i] != deserialized.reg[i] {
			t.Errorf("Expected reg to be the same, got %d at index %d", deserialized.reg[i], i)
		}
	}
}

// TestMaxPrecisionEstimate tests HLL_MAX_PRECISION on 20 000 000 added strings.
func TestMaxPrecisionEstimate(t *testing.T) {
	hll := NewHyperLogLog(HLL_MAX_PRECISION)
	for i := 0; i < 1_000_000; i++ {
		hll.Add([]byte("Mirko"))
		hll.Add([]byte("Filip"))
		hll.Add([]byte("Luka"))
		hll.Add([]byte("Leon"))
		hll.Add([]byte("Uros"))
		hll.Add([]byte("Mirko"))
		hll.Add([]byte("Stefan"))
		hll.Add([]byte("Marko"))
		hll.Add([]byte("Filip"))
		hll.Add([]byte("Lazar"))
		hll.Add([]byte("Lazar"))
		hll.Add([]byte("Milan"))
		hll.Add([]byte("Aleksa"))
		hll.Add([]byte("Lazar"))
		hll.Add([]byte("Milos"))
		hll.Add([]byte("Milos"))
		hll.Add([]byte("Milos"))
		hll.Add([]byte("Nikola"))
		hll.Add([]byte("Nikola"))
		hll.Add([]byte("Milos"))
	}
	estimate := math.Round(hll.Estimate())
	if estimate != 12 {
		t.Errorf("Expected 12 got %.0f", estimate)
	}
}
