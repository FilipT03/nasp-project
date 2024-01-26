package sstable

import (
	"bytes"
	"encoding/binary"
	"nasp-project/structures/compression"
	"nasp-project/util"
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
	util.BinaryFile // Only file block because nothing is ever loaded into memory
}

func (ir *IndexRecord) sizeOnDisk(compressionDict *compression.Dictionary) int {
	buf := make([]byte, binary.MaxVarintLen64)
	if compressionDict != nil {
		return binary.PutUvarint(buf, uint64(compressionDict.GetIdx(ir.Key))) + binary.PutUvarint(buf, uint64(ir.Offset))
	}
	return binary.PutUvarint(buf, uint64(len(ir.Key))) + len(ir.Key) + binary.PutUvarint(buf, uint64(ir.Offset))
}

// CreateFromDataBlock creates an index block from the given data block and writes it to disk.
// It also sets the size of the index block.
// sparseDeg is the number of records to skip before adding the next record.
// First and last records are always added.
func (ib *IndexBlock) CreateFromDataBlock(sparseDeg int, db *DataBlock, compressionDict *compression.Dictionary) error {
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
		var recSize int64 = 4
		offset, err := dbFile.Seek(4, 1) // skip CRC
		if err != nil {
			return err
		}
		if offset >= db.StartOffset+db.Size {
			break
		}

		_, n, err := util.ReadUvarintLen(dbFile) // skip timestamp
		if err != nil {
			return err
		}
		recSize += int64(n)

		tombstone := make([]byte, 1)
		rl, err := dbFile.Read(tombstone)
		if rl != 1 {
			break
		}
		if err != nil {
			return err
		}
		recSize += 1

		var keySize uint64 // only if compression is turned off
		if compressionDict == nil {
			keySize, err = util.ReadUvarint(file)
			if err != nil {
				return err
			}
			recSize += int64(n)
		}

		var valueSize uint64 = 0
		if tombstone[0] == 0 {
			valueSize, n, err = util.ReadUvarintLen(dbFile)
			if err != nil {
				return err
			}
			recSize += int64(n)
		}

		var key []byte
		if compressionDict == nil {
			// compression is off, read the key as-is
			key = make([]byte, keySize)
			_, err = file.Read(key)
			if err != nil {
				return err
			}
			recSize += int64(keySize)
		} else {
			// compression is on, get the key from compression dictionary
			keyIdx, n, err := util.ReadUvarintLen(file)
			if err != nil {
				return err
			}
			key = compressionDict.GetKey(int(keyIdx))
			recSize += int64(n)
		}

		offset, err = dbFile.Seek(int64(valueSize), 1)
		if err != nil {
			return err
		}
		recSize += int64(valueSize)

		offset -= db.StartOffset
		last := offset >= db.Size // whether this is the last record

		if cnt%sparseDeg == 0 || last { // add every sparseDeg-th record and the last record
			offset -= recSize
			err = ib.writeRecord(file, IndexRecord{key, offset}, compressionDict)
			if err != nil {
				return err
			}
		}
	}

	ib.Size, err = file.Seek(0, 1)
	if err != nil {
		return err
	}
	ib.Size -= ib.StartOffset
	return nil
}

// CreateFromDataRecords creates an index block for the data block that would be created from the given records and writes it to disk.
// It also sets the size of the index block.
// sparseDeg is the number of records to skip before adding the next record.
// First and last records are always added.
func (ib *IndexBlock) CreateFromDataRecords(sparseDeg int, recs []DataRecord, compressionDict *compression.Dictionary) ([]IndexRecord, error) {
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
			err = ib.writeRecord(file, ir, compressionDict)
			if err != nil {
				return idxRecs, err
			}
			idxRecs = append(idxRecs, ir)
		}
		offset += int64(rec.sizeOnDisk(compressionDict))
	}

	ib.Size, err = file.Seek(0, 1)
	if err != nil {
		return nil, err
	}
	ib.Size -= ib.StartOffset
	return idxRecs, nil
}

// writeRecord write a single IndexRecord to the given file.
func (ib *IndexBlock) writeRecord(file *os.File, ir IndexRecord, compressionDict *compression.Dictionary) error {
	if compressionDict == nil { // compression off
		// key-size
		err := util.WriteUvarint(file, uint64(len(ir.Key)))
		if err != nil {
			return err
		}
		// key
		_, err = file.Write(ir.Key)
		if err != nil {
			return err
		}
	} else { // compression on
		err := util.WriteUvarint(file, uint64(compressionDict.GetIdx(ir.Key)))
		if err != nil {
			return err
		}
	}

	err := util.WriteUvarint(file, uint64(ir.Offset))
	if err != nil {
		return err
	}

	return nil
}

// getRecordAtOffset returns the IndexRecord at the given offset in the index block file.
func (ib *IndexBlock) getRecordAtOffset(file *os.File, offset int64, compressionDict *compression.Dictionary) (*IndexRecord, error) {
	_, err := file.Seek(ib.StartOffset+offset, 0)
	if err != nil {
		return nil, err
	}

	var key []byte
	if compressionDict == nil { // compression off
		// key-size
		keySize, err := util.ReadUvarint(file)
		if err != nil {
			return nil, err
		}
		// key
		key = make([]byte, keySize)
		_, err = file.Read(key)
		if err != nil {
			return nil, err
		}
	} else { // compression on
		keyIdx, err := util.ReadUvarint(file)
		if err != nil {
			return nil, err
		}
		key = compressionDict.GetKey(int(keyIdx))
	}

	irOffset, err := util.ReadUvarint(file)
	if err != nil {
		return nil, err
	}

	return &IndexRecord{key, int64(irOffset)}, nil
}

// GetRecordWithKeyFromOffset returns the IndexRecord with the largest key that is less than or
// equal to the given key, starting from the given offset.
// Returns the record if the key is found, or nil if the key is not found.
// Returns an error if there is an error while reading the index block.
func (ib *IndexBlock) GetRecordWithKeyFromOffset(key []byte, offset int64, compressionDict *compression.Dictionary) (*IndexRecord, error) {
	file, err := os.Open(ib.Filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lastFoundRecord *IndexRecord = nil
	for {
		idxRec, err := ib.getRecordAtOffset(file, offset, compressionDict)
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
		offset += int64(idxRec.sizeOnDisk(compressionDict)) // offset of the next record
		if offset >= ib.Size {
			return lastFoundRecord, nil
		}
	}
}
