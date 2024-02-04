package bitset

import "encoding/binary"

// Bitset is a bitset of size n.
type Bitset struct {
	bitset []byte
	n      uint // number of bits
}

// NewBitset creates a new Bitset of size n with all bits set to 0.
func NewBitset(n uint) *Bitset {
	return &Bitset{
		make([]byte, (n+7)/8),
		n,
	}
}

// Set the bit at index idx to 1 if val is non-zero, and to 0 if val is 0.
func (b *Bitset) Set(idx uint, val int8) {
	if val != 0 {
		b.bitset[idx>>3] |= 1 << (idx & 7)
	} else {
		b.bitset[idx>>3] &= 0xff ^ (1 << (idx & 7))
	}
}

// Get returns the bit at index idx
func (b *Bitset) Get(idx uint) int8 {
	if b.bitset[idx>>3]&(1<<(idx&7)) != 0 {
		return 1
	} else {
		return 0
	}
}

func (b *Bitset) Serialize() []byte {
	bytes := make([]byte, 4+len(b.bitset))
	binary.LittleEndian.PutUint32(bytes[0:4], uint32(b.n))
	copy(bytes[4:], b.bitset)
	return bytes
}

func Deserialize(data []byte) *Bitset {
	n := binary.LittleEndian.Uint32(data[0:4])
	return &Bitset{
		data[4:],
		uint(n),
	}
}
