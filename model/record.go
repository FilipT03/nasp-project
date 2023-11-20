package model

type Record struct {
	Key       []byte
	Value     []byte
	Tombstone bool
	Timestamp uint64
}
