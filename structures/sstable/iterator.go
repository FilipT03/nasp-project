package sstable

import (
	"bytes"
	"nasp-project/model"
	"nasp-project/structures/compression"
	"nasp-project/util"
	"os"
)

// Iterator through the records of an SSTable.
type Iterator struct {
	table           *SSTable
	offset          int64         // offset of the NEXT record in DataBlock
	record          *model.Record // current record, nil if the end is reached
	compressionDict *compression.Dictionary
}

func (sst *SSTable) NewIterator(compressionDict *compression.Dictionary) (*Iterator, error) {
	rec, offset, err := sst.GetFirstRecord(compressionDict)
	if err != nil {
		return nil, err
	}
	return &Iterator{
		table:           sst,
		offset:          offset,
		record:          rec,
		compressionDict: compressionDict,
	}, nil
}

// Next moves the iterator to the next record and returns false if the move fails (because the end was reached or an error occurred)
// Skips reserved keys. The key is reserved is util.IsReservedKey return true.
// TODO: Refactor to return an error
func (it *Iterator) Next() bool {
	file, err := os.Open(it.table.Data.Filename)
	if err != nil {
		return false
	}

	_, err = file.Seek(it.offset, 0)
	if err != nil {
		return false
	}

	var dr *DataRecord
	for {
		dr, err = it.table.Data.getNextRecord(file, it.compressionDict)
		if err != nil {
			return false
		}
		if dr == nil || !util.IsReservedKey(dr.Key) {
			break
		}
	}

	it.offset, err = file.Seek(0, 1)
	if err != nil {
		return false
	}

	if dr == nil { // reached the end
		it.table = nil
		it.record = nil
		it.compressionDict = nil
		return false
	}

	it.record = &model.Record{
		Key:       dr.Key,
		Value:     dr.Value,
		Timestamp: dr.Timestamp,
		Tombstone: dr.Tombstone,
	}
	return true
}

func (it *Iterator) Value() *model.Record {
	return it.record
}

// RangeIterator iterates through records in the SSTable in the range [startKey, endKey].
type RangeIterator struct {
	Iterator
	startKey []byte
	endKey   []byte
}

func (sst *SSTable) NewRangeIterator(startKey, endKey []byte, compressionDict *compression.Dictionary) (*RangeIterator, error) {
	rec, offset, err := sst.GetNextRecordAtKey(startKey, compressionDict)
	if err != nil {
		return nil, err
	}

	if rec == nil || bytes.Compare(rec.Key, endKey) > 0 {
		return &RangeIterator{
			Iterator{
				table:           nil,
				offset:          offset,
				record:          nil,
				compressionDict: nil,
			},
			startKey,
			endKey,
		}, nil
	}

	return &RangeIterator{
		Iterator{
			table:           sst,
			offset:          offset,
			record:          rec,
			compressionDict: compressionDict,
		},
		startKey,
		endKey,
	}, nil
}

func (it *RangeIterator) Next() bool {
	if !it.Iterator.Next() {
		return false
	}
	if bytes.Compare(it.Value().Key, it.endKey) > 0 {
		it.table = nil
		it.record = nil
		it.compressionDict = nil
		return false
	}
	return true
}

func (it *RangeIterator) Value() *model.Record {
	return it.Iterator.Value()
}

// PrefixIterator iterates through records in the SSTable with the given prefix.
type PrefixIterator struct {
	Iterator
	prefix []byte
}

func (sst *SSTable) NewPrefixIterator(prefix []byte, compressionDict *compression.Dictionary) (*PrefixIterator, error) {
	rec, offset, err := sst.GetNextRecordAtKey(prefix, compressionDict)
	if err != nil {
		return nil, err
	}

	if rec == nil || !bytes.HasPrefix(rec.Key, prefix) {
		return &PrefixIterator{
			Iterator{
				table:           nil,
				offset:          offset,
				record:          nil,
				compressionDict: nil,
			},
			prefix,
		}, nil
	}

	return &PrefixIterator{
		Iterator{
			table:           sst,
			offset:          offset,
			record:          rec,
			compressionDict: compressionDict,
		},
		prefix,
	}, nil
}

func (it *PrefixIterator) Next() bool {
	if !it.Iterator.Next() {
		return false
	}
	if !bytes.HasPrefix(it.Value().Key, it.prefix) {
		it.table = nil
		it.record = nil
		it.compressionDict = nil
		return false
	}
	return true
}

func (it *PrefixIterator) Value() *model.Record {
	return it.Iterator.Value()
}
