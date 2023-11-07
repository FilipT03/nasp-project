package sstable

import (
	bytesUtil "bytes"
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

// getRecordAtOffset reads a record from the data block file at the given offset.
func (db *DataBlock) getRecordAtOffset(offset int64) (*DataRecord, error) {
	file, err := os.Open(db.Filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	_, err = file.Seek(db.StartOffset+offset, 0)
	if err != nil {
		return nil, err
	}

	bytes := make([]byte, 4)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}
	crc := binary.LittleEndian.Uint32(bytes)

	bytes = make([]byte, 8)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}
	timestamp := binary.LittleEndian.Uint64(bytes)

	bytes = make([]byte, 1)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}
	tombstone := bytes[0] == 1

	bytes = make([]byte, 8)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}
	keySize := binary.LittleEndian.Uint64(bytes)

	var valueSize uint64
	if !tombstone {
		bytes = make([]byte, 8)
		_, err = file.Read(bytes)
		if err != nil {
			return nil, err
		}
		valueSize = binary.LittleEndian.Uint64(bytes)
	}

	key := make([]byte, keySize)
	_, err = file.Read(key)
	if err != nil {
		return nil, err
	}

	var value []byte
	if !tombstone {
		value = make([]byte, valueSize)
		_, err = file.Read(value)
		if err != nil {
			return nil, err
		}
	}

	return &DataRecord{
		CRC:       crc,
		Tombstone: tombstone,
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}, nil
}

// GetRecordWithKeyFromOffset reads a record with the given key from the data block file, starting from the offset.
// Returns nil if the record is not found.
func (db *DataBlock) GetRecordWithKeyFromOffset(key []byte, offset int64) (*DataRecord, error) {
	// TODO: Add CRC check
	file, err := os.Open(db.Filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	_, err = file.Seek(db.StartOffset+offset, 0)
	if err != nil {
		return nil, err
	}

	for {
		dataRec, err := db.getRecordAtOffset(offset)
		if err != nil {
			return nil, err
		}
		if dataRec == nil {
			return nil, nil
		}
		cmp := bytesUtil.Compare(dataRec.Key, key)
		if cmp == 0 {
			return dataRec, nil
		} else if cmp > 0 {
			return nil, nil
		}
		if dataRec.Tombstone {
			offset += 21 + int64(len(dataRec.Key))
		} else {
			offset += 29 + int64(len(dataRec.Key)) + int64(len(dataRec.Value))
		}
	}
}
