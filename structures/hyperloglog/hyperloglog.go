package hyperloglog

import (
	"encoding/binary"
	"math"
	"math/bits"
	"nasp-project/structures/hash"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

// HyperLogLog struct where m is set size, p precision, reg array of m elements.
type HyperLogLog struct {
	m    uint32 // set size
	p    uint32 // precision
	reg  []uint32
	hash hash.SeededHash
}

// NewHyperLogLogWithSize returns a new HyperLogLog instance with set of size m.
func NewHyperLogLogWithSize(m uint32) *HyperLogLog {
	return &HyperLogLog{
		m:    m,
		p:    uint32(math.Ceil(math.Log2(float64(m)))),
		reg:  make([]uint32, m),
		hash: hash.CreateHashes(1)[0],
	}
}

// NewHyperLogLogWithPrecision returns a new HyperLogLog instance with precision of p.
func NewHyperLogLogWithPrecision(p uint) *HyperLogLog {
	m := uint32(math.Ceil(math.Pow(2, float64(p))))
	return &HyperLogLog{
		m:    m,
		p:    uint32(math.Ceil(math.Log2(float64(m)))),
		reg:  make([]uint32, m),
		hash: hash.CreateHashes(1)[0],
	}
}

// Add adds a key to the set.
func (hll *HyperLogLog) Add(data []byte) {
	hashedData := hll.hash.Hash(data)
	bucket := hashedData >> (64 - hll.p)
	value := 1 + bits.TrailingZeros64(hashedData)
	hll.reg[bucket] = uint32(value)
}

// Estimate estimates distinct values in set.
func (hll *HyperLogLog) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}
	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) {
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) {
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HyperLogLog) Serialize() []byte {
	bytes := make([]byte, 8+4*hll.m+8)
	binary.LittleEndian.PutUint32(bytes[0:4], hll.p)
	binary.LittleEndian.PutUint32(bytes[4:8], hll.m)

	for i := uint32(0); i < hll.m; i++ {
		binary.LittleEndian.PutUint32(bytes[8+4*i:8+4*i+4], hll.reg[i])
	}
	bytes = append(bytes, hll.hash.Seed...)

	return bytes
}

func Deserialize(data []byte) *HyperLogLog {
	p := binary.LittleEndian.Uint32(data[0:4])
	m := binary.LittleEndian.Uint32(data[4:8])
	reg := make([]uint32, m)

	for i := uint32(0); i < m; i++ {
		reg[i] = binary.LittleEndian.Uint32(data[8+4*i : 8+4*i+4])
	}

	return &HyperLogLog{
		m:    m,
		p:    p,
		reg:  reg,
		hash: hash.NewSeededHash(binary.LittleEndian.Uint64(data[8+4*m+8 : 8+4*m+8+8])),
	}
}

// emptyCount counts 0-s in set.
func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}
