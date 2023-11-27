package sim_hash

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"nasp-project/structures/hash"
	"regexp"
	"strings"
)

var seededHash = hash.NewSeededHash(14_02_2003)

// Regular expression to separate words from text.
var wordsRe = regexp.MustCompile(`\b[^\W\d][\w'-]*\b`)

type Fingerprint uint64
type Vector [64]int

type Feature interface {
	// HashValue Returns the 64-bit hash value of this feature.
	HashValue() uint64
	// Weight Returns the weight of this feature.
	Weight() int
}

// FeatureSet Represents a collection of features that describe a single document.
type FeatureSet interface {
	GetFeaturesSlice() []Feature
}

// ---

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

// VectorFromFeatures Returns a 64 dimension vector given a set of features.
// All the dimensions of the vector are set to 0. Then the i-th dimension of the vector is
// incremented by the weight of the feature if the i-th bit of the feature is 1, and decremented
// by the weight of the feature otherwise.
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

// VectorFromBytes Returns a 64 dimension vector given a set of [][]byte,
// where each []byte represents a single feature with weight 1.
func VectorFromBytes(b [][]byte) Vector {
	var vector Vector

	for _, feature := range b {
		hashValue := seededHash.Hash(feature)
		for i := uint8(0); i < 64; i++ {
			if ((hashValue >> i) & 1) == 1 {
				vector[i]++
			} else {
				vector[i]--
			}
		}
	}

	return vector
}

// GetFingerprint Returns a Fingerprint (uint64).
// The i-th bit fo the fingerprint is 1 if the i-th dimension of vector is positive, 0 otherwise.
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

// SimHash Returns a Fingerprint that represents a 64 bit simhash of the given FeatureSet.
func SimHash(features FeatureSet) Fingerprint {
	return SimHashFeatures(features.GetFeaturesSlice())
}

// SimHash Returns a Fingerprint that represents a 64 bit simhash of the given features.
func SimHashFeatures(features []Feature) Fingerprint {
	return GetFingerprint(VectorFromFeatures(features))
}

// SimHashBytes Returns a Fingerprint that represents a 64 bit simhash of the given bytes.
// All the features that are represented by []byte are assumed to have the same weight 1.
func SimHashBytes(features [][]byte) Fingerprint {
	return GetFingerprint(VectorFromBytes(features))
}

// SimHashText Calculates a simhash for the given text. One word corresponds to one feature and the weight
// of the feature is the number of occurences of its word in the text. Returns error if no words are found in the given text.
func SimHashText(text string) (Fingerprint, error) {
	words := wordsRe.FindAllString(text, -1)

	if len(words) == 0 {
		return 0, errors.New("No words found in the provided text.")
	}

	// TODO maby add stopword removal

	features := map[string]*feature{}

	for _, word := range words {
		word = strings.ToLower(word)

		if _, ok := features[word]; !ok {
			newFeature := NewFeatureWithWeight([]byte(word), 0)
			features[word] = &newFeature
		}

		features[word].weight++
	}

	var featuresSlice []Feature
	for _, feature := range features {
		featuresSlice = append(featuresSlice, *feature)
	}

	return SimHashFeatures(featuresSlice), nil
}

// ---

// HammingDistance Calculates the hamming distance between two Fingerprint x and y.
func HammingDistance(x, y Fingerprint) uint8 {
	return uint8(bits.OnesCount64(uint64(x ^ y)))
}

// HammingDistanceFrom Calculates the Hamming distance between this Fingerprint and y.
func (f Fingerprint) HammingDistanceFrom(y Fingerprint) uint8 {
	return HammingDistance(f, y)
}

// ---

func (f Fingerprint) Serialize() []byte {
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, uint64(f))
	return bytes
}

func Serialize(f Fingerprint) []byte {
	return f.Serialize()
}

func Deserialize(data []byte) Fingerprint {
	return Fingerprint(binary.LittleEndian.Uint64(data[0:8]))
}
