package sstable

import (
	bytesUtil "bytes"
	"encoding/binary"
	"os"
)

/*
	=== SUMMARY BLOCK ===

	+------------------------+-------------+----------------------+-----------+-...-+--...--+--...--+
	|   Start Key Size (8B)  |  Start Key  |   End Key Size (8B)  |  End Key  |  SummaryBlock records
	+------------------------+-------------+----------------------+-----------+-...-+--...--+--...--+
	Start Key Size = Length of the Start Key data
	Start Key = Start Key data
	End Key Size = Length of the End Key data
	End Key = End Key data
	Summary records = List of Index Records that are in the data block

	NOTE: Summary records are sorted by Key
*/

type SummaryRecord struct {
	Key    []byte
	Offset int64
}

type SummaryBlock struct {
	Filename    string
	StartOffset int64
	Size        int64
	StartKey    []byte
	EndKey      []byte
	Records     []SummaryRecord
}

// HasLoaded returns true if the summary block has been loaded into memory.
func (sb *SummaryBlock) HasLoaded() bool {
	return sb.Records != nil
}

// HasRangeLoaded returns true if the start and end keys have been loaded into memory.
func (sb *SummaryBlock) HasRangeLoaded() bool {
	return sb.StartKey != nil && sb.EndKey != nil
}

// LoadRange reads the start and end keys from the summary block file and loads them into memory.
func (sb *SummaryBlock) LoadRange() error {
	file, err := os.Open(sb.Filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(sb.StartOffset, 0)
	if err != nil {
		return err
	}

	startSizeBytes := make([]byte, 8)
	_, err = file.Read(startSizeBytes)
	if err != nil {
		return err
	}
	startSize := binary.LittleEndian.Uint64(startSizeBytes)

	sb.StartKey = make([]byte, startSize)
	_, err = file.Read(sb.StartKey)
	if err != nil {
		return err
	}

	endSizeBytes := make([]byte, 8)
	_, err = file.Read(endSizeBytes)
	if err != nil {
		return err
	}
	endSize := binary.LittleEndian.Uint64(endSizeBytes)

	sb.EndKey = make([]byte, endSize)
	_, err = file.Read(sb.EndKey)
	if err != nil {
		return err
	}

	return nil
}

// Load reads the summary block from disk and loads it into memory.
func (sb *SummaryBlock) Load() error {
	if !sb.HasRangeLoaded() {
		err := sb.LoadRange()
		if err != nil {
			return err
		}
	}

	file, err := os.Open(sb.Filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(sb.StartOffset+16+int64(len(sb.StartKey))+int64(len(sb.EndKey)), 0)
	if err != nil {
		return err
	}

	sb.Records = make([]SummaryRecord, 0)

	for {
		keySizeBytes := make([]byte, 8)
		rl, err := file.Read(keySizeBytes)
		if rl != 8 {
			break
		}
		if err != nil {
			return err
		}
		keySize := binary.LittleEndian.Uint64(keySizeBytes)

		key := make([]byte, keySize)
		_, err = file.Read(key)
		if err != nil {
			return err
		}

		offsetBytes := make([]byte, 8)
		_, err = file.Read(offsetBytes)
		if err != nil {
			return err
		}
		offset := int64(binary.LittleEndian.Uint64(offsetBytes))

		sb.Records = append(sb.Records, SummaryRecord{key, offset})

		pos, err := file.Seek(0, 1)
		if err != nil {
			return err
		}
		if pos >= sb.Size {
			break
		}
	}

	return nil
}

// CreateFromIndexBlock creates a summary block from the given index block and writes it to disk.
// The sparseDeg parameter determines how many index records are skipped between each summary record.
// It also sets the size of the summary block.
// Note: The summary block is not loaded into memory.
func (sb *SummaryBlock) CreateFromIndexBlock(sparseDeg int, ib *IndexBlock) error {
	ibFile, err := os.Open(ib.Filename)
	if err != nil {
		return err
	}
	defer ibFile.Close()

	_, err = ibFile.Seek(ib.StartOffset, 0)
	if err != nil {
		return err
	}

	var bytes []byte // Summary Records
	startSizeBytes := make([]byte, 8)
	var startKey []byte = nil
	endSizeBytes := make([]byte, 8)
	var endKey []byte

	for cnt := 0; ; cnt++ {
		keySizeBytes := make([]byte, 8)
		rl, err := ibFile.Read(keySizeBytes)
		if rl != 8 {
			break
		}
		if err != nil {
			return err
		}
		keySize := binary.LittleEndian.Uint64(keySizeBytes)

		key := make([]byte, keySize)
		_, err = ibFile.Read(key)
		if err != nil {
			return err
		}

		if cnt%sparseDeg == 0 {
			offset, err := ibFile.Seek(8, 1) // Skip over the offset
			if err != nil {
				return err
			}
			offset -= 16 + int64(len(key)) // Start of the record

			sr := SummaryRecord{
				Key:    key,
				Offset: offset - ib.StartOffset,
			}
			bytes = append(bytes, sb.writeRecord(sr)...)

			if startKey == nil { // First key
				binary.LittleEndian.PutUint64(startSizeBytes, uint64(len(key)))
				startKey = key
			}
		}

		// Last key
		binary.LittleEndian.PutUint64(endSizeBytes, uint64(len(key)))
		endKey = key
	}

	// Open the file and write the summary block
	file, err := os.OpenFile(sb.Filename, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(sb.StartOffset+16, 0)
	if err != nil {
		return err
	}

	// Start Key
	_, err = file.Write(startSizeBytes)
	if err != nil {
		return err
	}
	_, err = file.Write(startKey)
	if err != nil {
		return err
	}

	// End Key
	_, err = file.Write(endSizeBytes)
	if err != nil {
		return err
	}
	_, err = file.Write(endKey)
	if err != nil {
		return err
	}

	// Summary Records
	_, err = file.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

// CreateFromIndexRecords creates a summary block from the given index records and writes it to disk.
// The sparseDeg parameter determines how many index records are skipped between each summary record.
// It also sets the size of the summary block.
// Note: The summary block is not loaded into memory.
func (sb *SummaryBlock) CreateFromIndexRecords(sparseDeg int, recs []IndexRecord) error {
	var bytes []byte
	sumRecs := make([]SummaryRecord, 0, len(recs)/sparseDeg)

	var offset int64 = 0
	for cnt, rec := range recs {
		if cnt%sparseDeg == 0 {
			sr := SummaryRecord{
				Key:    rec.Key,
				Offset: offset,
			}
			sumRecs = append(sumRecs, sr)
			cb := sb.writeRecord(sr)
			bytes = append(bytes, cb...)
		}
		offset += 16 + int64(len(rec.Key))
	}

	// Open the file and write the summary block
	file, err := os.OpenFile(sb.Filename, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(sb.StartOffset, 0)
	if err != nil {
		return err
	}

	// Start Key
	startSizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(startSizeBytes, uint64(len(sumRecs[0].Key)))
	_, err = file.Write(startSizeBytes)
	if err != nil {
		return err
	}

	_, err = file.Write(sumRecs[0].Key)
	if err != nil {
		return err
	}

	// End Key
	endSizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(endSizeBytes, uint64(len(recs[len(recs)-1].Key)))
	_, err = file.Write(endSizeBytes)
	if err != nil {
		return err
	}

	_, err = file.Write(recs[len(recs)-1].Key)
	if err != nil {
		return err
	}

	// Summary Records
	_, err = file.Write(bytes)

	// Set the size of the summary block
	sb.Size, err = file.Seek(0, 1)
	if err != nil {
		return err
	}
	sb.Size -= sb.StartOffset

	return nil
}

// writeRecord writes a single IndexRecord to a byte slice.
func (sb *SummaryBlock) writeRecord(sr SummaryRecord) []byte {
	bytes := make([]byte, 16+len(sr.Key))

	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, uint64(len(sr.Key)))
	copy(bytes[0:8], keySize)

	copy(bytes[8:8+len(sr.Key)], sr.Key)

	offsetBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(offsetBytes, uint64(sr.Offset))
	copy(bytes[8+len(sr.Key):], offsetBytes)

	return bytes
}

// GetIndexOffset returns the SummaryRecord with the largest key that is less than or equal to the given key.
// It returns nil if the key is not found.
// Note: If the summary block is not loaded into memory, it will be loaded.
// It returns an error if the summary block cannot be read.
func (sb *SummaryBlock) GetIndexOffset(key []byte) (*SummaryRecord, error) {
	if !sb.HasLoaded() {
		err := sb.Load()
		if err != nil {
			return nil, err
		}
	}

	// Binary search
	l := 0
	r := len(sb.Records) - 1
	lp := -1
	for l <= r {
		m := l + (r-l)/2
		if bytesUtil.Compare(sb.Records[m].Key, key) <= 0 {
			lp = m
			l = m + 1
		} else {
			r = m - 1
		}
	}

	if lp == -1 {
		return nil, nil
	} else {
		return &sb.Records[lp], nil
	}
}
