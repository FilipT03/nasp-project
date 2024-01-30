package memtable

import (
	"bytes"
	"nasp-project/model"
	"nasp-project/util"
)

// RangeScan returns records from memtables within the inclusive range [minValue, maxValue].
func (mts *Memtables) RangeScan(startValue []byte, endValue []byte) []*model.Record {
	iterators := mts.GetRangeIterator(startValue, endValue)
	records := make([]*model.Record, 0)

	seenValues := make(map[string]bool)
	for {
		minIndex := -1
		minKey := []byte{255}
		maxTimestamp := uint64(0)

		for i, iter := range iterators {
			if isMinimalKey(iter, minKey, maxTimestamp) {
				minIndex = i
				minKey = iter.Value().Key
				maxTimestamp = iter.Value().Timestamp
			}
		}

		if minIndex == -1 {
			break
		}

		if !seenValues[string(minKey)] {
			if bytes.Compare(iterators[minIndex].Value().Key, endValue) > 0 {
				iterators[minIndex] = nil
				continue
			}
			records = append(records, iterators[minIndex].Value())
			seenValues[string(minKey)] = true
		}

		iterators[minIndex].Next()
	}
	return records
}

// PrefixScan returns records from memtables that have the specified prefix.
func (mts *Memtables) PrefixScan(prefix []byte) []*model.Record {
	records := make([]*model.Record, 0)
	if util.IsReservedKey(prefix) {
		return records
	}
	iterators := mts.GetPrefixIterator(prefix)

	seenValues := make(map[string]bool)
	for {
		minIndex := -1
		minKey := []byte{255}
		maxTimestamp := uint64(0)

		for i, iter := range iterators {
			if isMinimalKey(iter, minKey, maxTimestamp) {
				minIndex = i
				minKey = iter.Value().Key
				maxTimestamp = iter.Value().Timestamp
			}
		}

		if minIndex == -1 {
			break
		}

		if !seenValues[string(minKey)] {
			if !bytes.HasPrefix(iterators[minIndex].Value().Key, prefix) {
				iterators[minIndex] = nil
				continue
			}
			records = append(records, iterators[minIndex].Value())
			seenValues[string(minKey)] = true
		}

		iterators[minIndex].Next()
	}

	return records
}
