package util

import (
	"hash/crc32"
	"time"
)

type DataRecord struct {
	CRC       uint32
	Tombstone bool
	Key       []byte
	Value     []byte
	Timestamp int64
}

// NewRecord creates a new instance of DataRecord.
func NewRecord(key []byte, value []byte) *DataRecord {
	return &DataRecord{
		CRC:       crc32.ChecksumIEEE(value),
		Tombstone: false,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().Unix(),
	}
}
