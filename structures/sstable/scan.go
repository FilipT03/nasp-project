package sstable

import (
	"nasp-project/model"
	"nasp-project/structures/compression"
)

// RangeScan returns records from the SSTable that have a key in range [startKey, endKey].
// If maxRecords is -1, returns all such records, else returns the first maxRecord records
// (or less if there is fewer records in total).
func (sst *SSTable) RangeScan(startKey, endKey []byte, maxRecords int, compressionDict *compression.Dictionary) ([]*model.Record, error) {
	var res []*model.Record
	numRecs := 0

	it, err := sst.NewRangeIterator(startKey, endKey, compressionDict)
	if err != nil {
		return nil, err
	}

	for (maxRecords == -1 || numRecs < maxRecords) && it.Value() != nil {
		res = append(res, it.Value())
		numRecs++
		it.Next()
	}

	return res, nil
}

// PrefixScan returns records from the SSTable that have a key starting with prefix.
// If maxRecords is -1, returns all such records, else returns the first maxRecord records
// (or less if there is fewer records in total).
func (sst *SSTable) PrefixScan(prefix []byte, maxRecords int, compressionDict *compression.Dictionary) ([]*model.Record, error) {
	var res []*model.Record
	numRecs := 0

	it, err := sst.NewPrefixIterator(prefix, compressionDict)
	if err != nil {
		return nil, err
	}

	for (maxRecords == -1 || numRecs < maxRecords) && it.Value() != nil {
		res = append(res, it.Value())
		numRecs++
		it.Next()
	}

	return res, nil
}
