package sstable

import (
	bytesUtil "bytes"
	"encoding/binary"
	"errors"
	"nasp-project/model"
	"nasp-project/util"
	"os"
)

/*
	=== DATA RECORD ===

	+---------------+------------------+---------------+----------------+------------------+-...-+--...--+
	|    CRC (4B)   | Timestamp (VAR)  | Tombstone(1B) | Key Size (VAR) | Value Size (VAR) | Key | Value |
	+---------------+------------------+---------------+----------------+------------------+-...-+--...--+
	CRC = 32bit hash computed over the payload using CRC
	Timestamp = Timestamp of the operation in seconds
	Tombstone = If this record was deleted
	Key Size = Length of the Key data
	Value Size = Length of the Value data (only if Tombstone is 0)
	Key = Key data
	Value = Value data (only if Tombstone is 0)

	NOTE: Value and Value Size are left out if Tombstone is set.
	NOTE: Fields marked with VAR are encoded using variable encoding and take up between 1 and 10 bytes.
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
	util.BinaryFile // Only file block because nothing is ever loaded into memory
}

// sizeOnDisk returns the number of bytes that DataRecord would occupy on disk.
func (dr *DataRecord) sizeOnDisk() int {
	buf := make([]byte, binary.MaxVarintLen64)
	res := 4 + binary.PutUvarint(buf, dr.Timestamp) + 1 + binary.PutUvarint(buf, uint64(len(dr.Key))) + len(dr.Key)
	if !dr.Tombstone {
		res += binary.PutUvarint(buf, uint64(len(dr.Value))) + len(dr.Value)
	}
	return res
}

// getCRC calculates the CRC checksum of a given record
func getCRC(record *model.Record) uint32 {
	bytes := make([]byte, 9)
	binary.LittleEndian.PutUint64(bytes[:8], record.Timestamp)
	if record.Tombstone {
		bytes[8] = 1
	} else {
		bytes[8] = 0
	}
	bytes = append(bytes, record.Key...)
	if !record.Tombstone {
		bytes = append(bytes, record.Value...)
	}
	return util.CRC32(bytes)
}

func (dr *DataRecord) isCRCValid() bool {
	rec := model.Record{
		Key:       dr.Key,
		Value:     dr.Value,
		Tombstone: dr.Tombstone,
		Timestamp: dr.Timestamp,
	}
	return getCRC(&rec) == dr.CRC
}

func dataRecordsFromRecords(recs []model.Record) []DataRecord {
	dataRecs := make([]DataRecord, len(recs))
	for i, rec := range recs {
		dataRecs[i] = DataRecord{
			Key:       rec.Key,
			Value:     rec.Value,
			Tombstone: rec.Tombstone,
			Timestamp: rec.Timestamp,
			CRC:       getCRC(&rec),
		}
	}
	return dataRecs
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
		err = db.writeRecord(file, &rec)
		if err != nil {
			return err
		}
	}

	db.Size, err = file.Seek(0, 1)
	if err != nil {
		return err
	}
	db.Size -= db.StartOffset

	return nil
}

// writeRecord writes a record to the data block file.
func (db *DataBlock) writeRecord(file *os.File, rec *DataRecord) error {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, rec.CRC)
	_, err := file.Write(bytes)
	if err != nil {
		return err
	}

	err = util.WriteUvarint(file, rec.Timestamp)
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

	err = util.WriteUvarint(file, uint64(len(rec.Key)))
	if err != nil {
		return err
	}

	if !rec.Tombstone {
		err = util.WriteUvarint(file, uint64(len(rec.Value)))
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

	return nil
}

// isEndOfBlock return true if file pointer is positioned at the end of the given data block.
func (db *DataBlock) isEndOfBlock(file *os.File) (bool, error) {
	pos, err := file.Seek(0, 1)
	if err != nil {
		return false, err
	}
	return pos == db.StartOffset+db.Size, nil
}

// getNextRecord assumes the provided file is at the start of the record and reads the next record.
// Returns nil if positioned at the end of data block.
func (db *DataBlock) getNextRecord(file *os.File) (*DataRecord, error) {
	end, err := db.isEndOfBlock(file)
	if err != nil {
		return nil, err
	}
	if end {
		return nil, nil
	}

	bytes := make([]byte, 4)
	rl, err := file.Read(bytes)
	if rl != 4 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	crc := binary.LittleEndian.Uint32(bytes)

	timestamp, err := util.ReadUvarint(file)
	if err != nil {
		return nil, err
	}

	bytes = make([]byte, 1)
	_, err = file.Read(bytes)
	if err != nil {
		return nil, err
	}
	tombstone := bytes[0] == 1

	keySize, err := util.ReadUvarint(file)

	var valueSize uint64
	if !tombstone {
		valueSize, err = util.ReadUvarint(file)
		if err != nil {
			return nil, err
		}
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

	rec := &DataRecord{
		CRC:       crc,
		Tombstone: tombstone,
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
	}

	if !rec.isCRCValid() {
		return rec, errors.New("CRC check failed")
	}
	return rec, nil
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

	return db.getNextRecord(file)
}

// GetRecordWithKeyFromOffset reads a record with the given key from the data block file, starting from the offset.
// Returns nil if the record is not found.
func (db *DataBlock) GetRecordWithKeyFromOffset(key []byte, offset int64) (*DataRecord, error) {
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
		dataRec, err := db.getNextRecord(file)
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
	}
}

// WriteMerged merges db1 and db2 and writes the result to db.
// It also sets the size of the new data block.
// Returns the number of records in the merged data block.
func (db *DataBlock) WriteMerged(db1, db2 *DataBlock) (uint, error) {
	file, err := os.OpenFile(db.Filename, os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	file1, err := os.Open(db1.Filename)
	if err != nil {
		return 0, err
	}
	defer file1.Close()

	file2, err := os.Open(db2.Filename)
	if err != nil {
		return 0, err
	}
	defer file2.Close()

	rec1, err := db1.getNextRecord(file1)
	if err != nil {
		return 0, err
	}
	rec2, err := db2.getNextRecord(file2)
	if err != nil {
		return 0, err
	}

	cnt := uint(0) // number of records in the merged data block
	for {
		if rec1 == nil && rec2 == nil {
			break
		} else if rec1 == nil {
			err = db.writeRecord(file, rec2)
			if err != nil {
				return cnt, err
			}
			rec2, err = db2.getNextRecord(file2)
			if err != nil {
				return cnt, err
			}
		} else if rec2 == nil {
			err = db.writeRecord(file, rec1)
			if err != nil {
				return cnt, err
			}
			rec1, err = db1.getNextRecord(file1)
			if err != nil {
				return cnt, err
			}
		} else {
			cmp := bytesUtil.Compare(rec1.Key, rec2.Key)
			if cmp < 0 {
				err = db.writeRecord(file, rec1)
				if err != nil {
					return cnt, err
				}
				rec1, err = db1.getNextRecord(file1)
				if err != nil {
					return cnt, err
				}
			} else if cmp > 0 {
				err = db.writeRecord(file, rec2)
				if err != nil {
					return cnt, err
				}
				rec2, err = db2.getNextRecord(file2)
				if err != nil {
					return cnt, err
				}
			} else if rec1.Timestamp > rec2.Timestamp {
				err = db.writeRecord(file, rec1)
				if err != nil {
					return cnt, err
				}
				rec1, err = db1.getNextRecord(file1)
				if err != nil {
					return cnt, err
				}
				rec2, err = db2.getNextRecord(file2)
				if err != nil {
					return cnt, err
				}
			} else {
				err = db.writeRecord(file, rec2)
				if err != nil {
					return cnt, err
				}
				rec1, err = db1.getNextRecord(file1)
				if err != nil {
					return cnt, err
				}
				rec2, err = db2.getNextRecord(file2)
				if err != nil {
					return cnt, err
				}
			}
		}
		cnt++
	}

	db.Size, err = file.Seek(0, 1)
	if err != nil {
		return cnt, err
	}
	db.Size -= db.StartOffset

	return cnt, nil
}
