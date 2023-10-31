package count_min_sketch

import (
	"math"
	"nasp-project/structures/hash"
)

type CMS struct {
	//Number of items expected to be stored in bloom filter
	itemsCount int
	//matrix
	matrix [][]byte
	rows   uint
	cols   uint
	seeds  []hash.SeededHash
}

func NewCMS(epsilon float64, delta float64) CMS {
	cols := CalculateM(epsilon)
	rows := CalculateK(delta)
	matrix := make([][]byte, rows)
	for i := range matrix {
		matrix[i] = make([]byte, cols)
	}
	seeds := hash.CreateHashes(rows)
	cms := CMS{
		itemsCount: 0,
		matrix:     matrix,
		rows:       rows,
		cols:       cols,
		seeds:      seeds,
	}
	return cms
}
func (cms CMS) Add(el string) {
	dataByte := []byte(el)
	for i := 0; i < len(cms.seeds); i++ {
		hashedVal := cms.seeds[i].Hash(dataByte) % uint64(cms.cols)
		cms.matrix[i][hashedVal] += 1

	}
}

func (cms CMS) Get(el string) int {
	dataByte := []byte(el)
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
