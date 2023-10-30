package hyperloglog

import (
	"math"
	"math/bits"
	"nasp-project/structures/hash"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HyperLogLog struct {
	m    uint64 // set size
	p    uint8  // precision
	reg  []uint
	hash hash.SeededHash
}

// NewHyperLogLogWithSize returns a new HyperLogLog instance with set of size m.
func NewHyperLogLogWithSize(m uint64) *HyperLogLog {
	return &HyperLogLog{
		m:    m,
		p:    uint8(math.Ceil(math.Log2(float64(m)))),
		reg:  make([]uint, m),
		hash: hash.CreateHashes(1)[0],
	}
}

// NewHyperLogLogWithPrecision returns a new HyperLogLog instance with set of precision m.
func NewHyperLogLogWithPrecision(p uint8) *HyperLogLog {
	m := uint64(math.Ceil(math.Pow(2, float64(p))))
	return &HyperLogLog{
		m:    m,
		p:    uint8(math.Ceil(math.Log2(float64(m)))),
		reg:  make([]uint, m),
		hash: hash.CreateHashes(1)[0],
	}
}

// Add adds a key to the set.
func (hll *HyperLogLog) Add(data []byte) {
	hashedData := hll.hash.Hash(data)
	bucket := hashedData >> (64 - hll.p)
	value := 1 + bits.TrailingZeros64(hashedData)
	hll.reg[bucket] = uint(value)
}
func (hll *HyperLogLog) Serialize() {}
func Deserialize() *HyperLogLog     { return nil }

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
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HyperLogLog) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}
