package count_min_sketch

import (
	"encoding/binary"
	"math"
	"nasp-project/structures/hash"
)

type CMS struct {
	matrix [][]byte
	rows   uint
	cols   uint
	seeds  []hash.SeededHash
}

// NewCMS returns a new CMS instance.
func NewCMS(epsilon float64, delta float64) *CMS {
	cols := CalculateM(epsilon)
	rows := CalculateK(delta)
	matrix := make([][]byte, rows)
	for i := range matrix {
		matrix[i] = make([]byte, cols)
	}
	seeds := hash.CreateHashes(rows)
	cms := CMS{
		matrix: matrix,
		rows:   rows,
		cols:   cols,
		seeds:  seeds,
	}
	return &cms
}

// Add adds an element to the matrix
func (cms *CMS) Add(dataByte []byte) {
	for i := 0; i < len(cms.seeds); i++ {
		hashedVal := cms.seeds[i].Hash(dataByte) % uint64(cms.cols)
		cms.matrix[i][hashedVal] += 1

	}
}

// Get returns the minimum count of the element in the matrix
func (cms *CMS) Get(dataByte []byte) int {
	minimum := 255
	for i := 0; i < len(cms.seeds); i++ {
		hashedVal := cms.seeds[i].Hash(dataByte) % uint64(cms.cols)
		minimum = min(int(cms.matrix[i][hashedVal]), minimum)

	}
	return minimum
}

// PARAMS

// CalculateM m - cols
func CalculateM(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

// CalculateK k - rows
func CalculateK(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

// SERIALIZATION

// Serialize serializes the CMS instance.
func (cms *CMS) Serialize() []byte {
	bytes := make([]byte, 8+8*cms.rows)
	binary.LittleEndian.PutUint32(bytes[0:4], uint32(cms.rows))
	binary.LittleEndian.PutUint32(bytes[4:8], uint32(cms.cols))
	for i := uint(0); i < cms.rows; i++ {
		copy(bytes[8+8*i:16+8*i], cms.seeds[i].Seed)

	}
	for i := uint(0); i < cms.rows; i++ {
		for j := uint(0); j < cms.cols; j++ {
			bytes = append(bytes, cms.matrix[i][j])
		}
	}
	return bytes
}

// Deserialize deserializes the CMS instance.
func Deserialize(data []byte) *CMS {
	rows := binary.LittleEndian.Uint32(data[0:4])
	cols := binary.LittleEndian.Uint32(data[4:8])
	seeds := make([]hash.SeededHash, rows)
	matrix := make([][]byte, rows)
	// Creating the empty matrix
	for i := range matrix {
		matrix[i] = make([]byte, cols)
	}
	for i := uint(0); i < uint(rows); i++ {
		seeds[i] = hash.NewSeededHash(binary.LittleEndian.Uint64(data[8+8*i : 16+8*i]))
	}
	for i := uint(0); i < uint(rows); i++ {
		for j := uint(0); j < uint(cols); j++ {
			matrix[i][j] = data[8+8*uint(rows)+i*uint(cols)+j]
		}
	}
	cms := CMS{
		matrix: matrix,
		rows:   uint(rows),
		cols:   uint(cols),
		seeds:  seeds,
	}
	return &cms
}
