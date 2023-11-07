package sstable

import (
	"encoding/binary"
	"os"
)

/*
	=== DATA RECORD ===

	+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
	|    CRC (4B)   | Timestamp (8B)  | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
	+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
	CRC = 32bit hash computed over the payload using CRC
	Timestamp = Timestamp of the operation in seconds
	Tombstone = If this record was deleted
	Key Size = Length of the Key data
	Value Size = Length of the Value data (only if Tombstone is 0)
	Key = Key data
	Value = Value data (only if Tombstone is 0)

	NOTE: Value and Value Size are left out if Tombstone is set.
	NOTE: Records are sorted by Key
*/

// DataRecord represents a record in an SSTable.
type DataRecord struct {
	CRC       uint32
	Tombstone bool
	Key       []byte
	Value     []byte
	Timestamp uint64
}

// DataBlock represents a data block in an SSTable.
type DataBlock struct {
	Filename    string // Where the data block is stored
	StartOffset int64  // Where the data block starts in the file (in bytes)
	Size        int64  // Size of the data block (in bytes)
}

// Write writes the records to the data block file.
// It also sets the size of the data block.
func (db *DataBlock) Write(recs []DataRecord) error {
	file, err := os.OpenFile(db.Filename, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Seek(db.StartOffset, 0)
	if err != nil {
		return err
	}

	for _, rec := range recs {
		bytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytes, rec.CRC)
		_, err = file.Write(bytes)
		if err != nil {
			return err
		}

		bytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, rec.Timestamp)
		_, err = file.Write(bytes)
		if err != nil {
			return err
		}

		if rec.Tombstone {
			_, err = file.Write([]byte{1})
			if err != nil {
				return err
			}
		} else {
			_, err = file.Write([]byte{0})
			if err != nil {
				return err
			}
		}

		bytes = make([]byte, 8)
		binary.LittleEndian.PutUint64(bytes, uint64(len(rec.Key)))
		_, err = file.Write(bytes)
		if err != nil {
			return err
		}

		if !rec.Tombstone {
			bytes = make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, uint64(len(rec.Value)))
			_, err = file.Write(bytes)
			if err != nil {
				return err
			}
		}

		_, err = file.Write(rec.Key)
		if err != nil {
			return err
		}

		if !rec.Tombstone {
			_, err = file.Write(rec.Value)
			if err != nil {
				return err
			}
		}
	}

	db.Size, err = file.Seek(0, 1)
	if err != nil {
		return err
	}
	db.Size -= db.StartOffset

	return nil
}
