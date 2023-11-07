package sstable

import (
	"bytes"
	"encoding/binary"
	"os"
)

/*
	=== INDEX RECORD ===

	+------------------+---------+---------------+
	|   Key Size (8B)  |   Key   |  Offset (8B)  |
	+------------------+---------+---------------+
	Value Size = Length of the Key data
	Key = Key data
	Offset = Number of bytes from the start of data block to the start of the record

	NOTE: Index records are sorted by Key
*/

type IndexRecord struct {
	Key    []byte
	Offset int64
}

type IndexBlock struct {
	Filename    string
	StartOffset int64
	Size        int64
}

// CreateFromDataBlock creates an index block from the given data block and writes it to disk.
// It also sets the size of the index block.
// sparseDeg is the number of records to skip before adding the next record.
// First and last records are always added.
func (ib *IndexBlock) CreateFromDataBlock(sparseDeg int, db *DataBlock) error {
	file, err := os.OpenFile(ib.Filename, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(ib.StartOffset, 0)
	if err != nil {
		return err
	}

	dbFile, err := os.Open(db.Filename)
	if err != nil {
		return err
	}
	defer dbFile.Close()

	_, err = dbFile.Seek(db.StartOffset, 0)
	if err != nil {
		return err
	}

	for cnt := 0; ; cnt++ {
		offset, err := dbFile.Seek(12, 1)
		if err != nil {
			return err
		}
		if offset >= db.Size {
			break
		}

		tombstone := make([]byte, 1)
		rl, err := dbFile.Read(tombstone)
		if rl != 1 {
			break
		}
		if err != nil {
			return err
		}

		keySizeBytes := make([]byte, 8)
		rl, err = dbFile.Read(keySizeBytes)
		if rl != 8 {
			break
		}
		if err != nil {
			return err
		}
		keySize := binary.LittleEndian.Uint64(keySizeBytes)

		var valueSize uint64 = 0
		if tombstone[0] == 0 {
			valueSizeBytes := make([]byte, 8)
			rl, err = dbFile.Read(valueSizeBytes)
			if rl != 8 {
				break
			}
			if err != nil {
				return err
			}
			valueSize = binary.LittleEndian.Uint64(valueSizeBytes)
		}

		key := make([]byte, keySize)
		rl, err = dbFile.Read(key)
		if rl != len(key) {
			break
		}
		if err != nil {
			return err
		}

		offset, err = dbFile.Seek(int64(valueSize), 1)
		if err != nil {
			return err
		}
		offset -= 21 + int64(len(key)) + int64(valueSize) // offset of the start of the record
		if tombstone[0] == 0 {
			offset -= 8 // value size field is not present if the record is a tombstone
		}
		offset -= db.StartOffset // offset of the start of the record relative to the start of the data block

		if cnt%sparseDeg == 0 || offset >= db.Size { // add every sparseDeg-th record and the last record
			err = ib.writeRecord(file, IndexRecord{key, offset})
			if err != nil {
				return err
			}
		}
	}

	ib.Size, err = file.Seek(0, 1)
	ib.Size -= ib.StartOffset
	return nil
}

// CreateFromDataRecords creates an index block for the data block that would be created from the given records and writes it to disk.
// It also sets the size of the index block.
// sparseDeg is the number of records to skip before adding the next record.
// First and last records are always added.
func (ib *IndexBlock) CreateFromDataRecords(sparseDeg int, recs []DataRecord) ([]IndexRecord, error) {
	file, err := os.OpenFile(ib.Filename, os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	idxRecs := make([]IndexRecord, 0, len(recs)/sparseDeg)

	_, err = file.Seek(ib.StartOffset, 0)
	if err != nil {
		return idxRecs, err
	}

	var offset int64 = 0
	for cnt, rec := range recs {
		if cnt%sparseDeg == 0 || cnt == len(recs)-1 { // add every sparseDeg-th record and the last record
			ir := IndexRecord{rec.Key, offset}
			err = ib.writeRecord(file, ir)
			if err != nil {
				return idxRecs, err
			}
			idxRecs = append(idxRecs, ir)
		}
		if rec.Tombstone {
			offset += 21 + int64(len(rec.Key))
		} else {
			offset += 29 + int64(len(rec.Key)) + int64(len(rec.Value))
		}
	}

	ib.Size, err = file.Seek(0, 1)
	ib.Size -= ib.StartOffset
	return idxRecs, nil
}

// writeRecord write a single IndexRecord to the given file.
func (ib *IndexBlock) writeRecord(file *os.File, ir IndexRecord) error {
	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, uint64(len(ir.Key)))
	_, err := file.Write(keySize)
	if err != nil {
		return err
	}

	_, err = file.Write(ir.Key)
	if err != nil {
		return err
	}

	offsetBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(ir.Offset))
	_, err = file.Write(offsetBytes)
	if err != nil {
		return err
	}

	return nil
}

// getRecordAtOffset returns the IndexRecord at the given offset in the index block file.
func (ib *IndexBlock) getRecordAtOffset(file *os.File, offset int64) (*IndexRecord, error) {
	_, err := file.Seek(ib.StartOffset+offset, 0)
	if err != nil {
		return nil, err
	}

	keySizeBytes := make([]byte, 8)
	_, err = file.Read(keySizeBytes)
	if err != nil {
		return nil, err
	}
	keySize := binary.LittleEndian.Uint64(keySizeBytes)

	key := make([]byte, keySize)
	_, err = file.Read(key)
	if err != nil {
		return nil, err
	}

	offsetBytes := make([]byte, 8)
	_, err = file.Read(offsetBytes)
	if err != nil {
		return nil, err
	}
	offset = int64(binary.LittleEndian.Uint64(offsetBytes))

	return &IndexRecord{key, offset}, nil
}

// GetRecordWithKeyFromOffset returns the IndexRecord with the largest key that is less than or
// equal to the given key, starting from the given offset.
// Returns the record if the key is found, or nil if the key is not found.
// Returns an error if there is an error while reading the index block.
func (ib *IndexBlock) GetRecordWithKeyFromOffset(key []byte, offset int64) (*IndexRecord, error) {
	file, err := os.Open(ib.Filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lastFoundRecord *IndexRecord = nil
	for {
		idxRec, err := ib.getRecordAtOffset(file, offset)
		if err != nil {
			return nil, err
		}
		if idxRec == nil {
			return lastFoundRecord, nil
		}
		cmp := bytes.Compare(idxRec.Key, key)
		if cmp == 0 {
			return idxRec, nil
		} else if cmp < 0 {
			lastFoundRecord = idxRec
		} else {
			return lastFoundRecord, nil
		}
		offset += 16 + int64(len(idxRec.Key)) // offset of the next record
		if offset >= ib.Size {
			return lastFoundRecord, nil
		}
	}
}
