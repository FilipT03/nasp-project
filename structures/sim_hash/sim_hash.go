package sim_hash

import (
	"encoding/binary"
	"math/bits"
	"math/rand"
	"nasp-project/structures/hash"
)

var seededHash = hash.NewSeededHash(uint64(rand.Uint32())<<32 + uint64(rand.Uint32()))

type Fingerprint uint64 // TODO: check if logic operators and bit shifts work with this
type Vector [64]int

type Feature interface {
	HashValue() uint64
	Weight() int
}

type FeatureSet interface {
	GetFeaturesSlice() []Feature
}

// ---

// feature Provides a default implementation for Feature interface.
type feature struct {
	hashValue uint64
	weight    int
}

func (f feature) HashValue() uint64 {
	return f.hashValue
}

func (f feature) Weight() int {
	return f.weight
}

// NewFeature Returns a new feature with the weight set to 1 and hashValue set to hash of b.
func NewFeature(b []byte) feature {
	return NewFeatureWithWeight(b, 1)
}

// NewFeatureWithWeight Returns a new feature with given weight and hashValue set to hash of b.
func NewFeatureWithWeight(b []byte, weight int) feature {
	return feature{
		hashValue: seededHash.Hash(b),
		weight:    weight,
	}
}

// ---

// VectorFromFeatures ...
func VectorFromFeatures(features []Feature) Vector {
	var vector Vector

	for _, feature := range features {
		hash := feature.HashValue()
		weight := feature.Weight()

		for i := uint8(0); i < 64; i++ {
			if ((hash >> i) & 1) == 0 {
				vector[i] -= weight
			} else {
				vector[i] += weight
			}
		}

	}

	return vector
}

// GetFingerprint Returns a Fingerprint (uint64)
// The i-th bit is 1 if the i-th dimension of vector is greater than 0
func GetFingerprint(vector Vector) Fingerprint {
	var print Fingerprint

	for i := uint8(0); i < 64; i++ {
		if vector[i] > 0 {
			print |= 1 << i
		}
	}

	return print
}

// ---

func SimHash(features FeatureSet) Fingerprint {
	return GetFingerprint(VectorFromFeatures(features.GetFeaturesSlice()))
}

// TODO add SimHashBytes

// HammingDistance Calculates the number of bits that x and y differ in
func HammingDistance(x, y Fingerprint) uint8 {
	return uint8(bits.OnesCount64(uint64(x ^ y)))
}

func (f Fingerprint) HammingDistanceFrom(y Fingerprint) uint8 {
	return HammingDistance(f, y)
}

// ---

func (f Fingerprint) Serialize() []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(f))
	return bytes
}

func Deserialize(data []byte) Fingerprint {
	return Fingerprint(binary.LittleEndian.Uint64(data[0:8]))
}
