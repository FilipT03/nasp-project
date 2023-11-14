package util

type DataRecord struct {
	Tombstone bool
	Key       []byte
	Value     []byte
	Timestamp int64
}
