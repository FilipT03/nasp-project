package sstable

import (
	bytesUtil "bytes"
	"encoding/binary"
	"nasp-project/util"
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
	util.BinaryFile
	StartKey []byte
	EndKey   []byte
	Records  []SummaryRecord
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

	startSize, err := util.ReadUvarint(file)
	if err != nil {
		return err
	}

	sb.StartKey = make([]byte, startSize)
	_, err = file.Read(sb.StartKey)
	if err != nil {
		return err
	}

	endSize, err := util.ReadUvarint(file)
	if err != nil {
		return err
	}

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

	buf := make([]byte, binary.MaxVarintLen64)
	_, err = file.Seek(
		sb.StartOffset+
			int64(binary.PutUvarint(buf, uint64(len(sb.StartKey))))+
			int64(binary.PutUvarint(buf, uint64(len(sb.EndKey))))+
			int64(len(sb.StartKey))+
			int64(len(sb.EndKey)),
		0)
	if err != nil {
		return err
	}

	sb.Records = make([]SummaryRecord, 0)

	for {
		keySize, err := util.ReadUvarint(file)
		if err != nil {
			return err
		}

		key := make([]byte, keySize)
		_, err = file.Read(key)
		if err != nil {
			return err
		}

		offset, err := util.ReadUvarint(file)
		if err != nil {
			return err
		}

		sb.Records = append(sb.Records, SummaryRecord{key, int64(offset)})

		pos, err := file.Seek(0, 1)
		if err != nil {
			return err
		}
		if pos >= sb.StartOffset+sb.Size {
			break
		}
	}

	return nil
}

// CreateFromIndexBlock creates a summary block from the given index block and writes it to disk.
// The sparseDeg parameter determines how many index records are skipped between each summary record.
// It also sets the size of the summary block.
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
	var startKey []byte = nil
	var endKey []byte

	for cnt := 0; ; cnt++ {
		var recSize int64 = 0
		keySize, n, err := util.ReadUvarintLen(ibFile)
		if err != nil {
			return err
		}
		if n == 0 {
			break
		}
		recSize += int64(n)

		key := make([]byte, keySize)
		_, err = ibFile.Read(key)
		if err != nil {
			return err
		}
		recSize += int64(keySize)

		_, n, err = util.ReadUvarintLen(ibFile)
		if err != nil {
			return err
		}
		recSize += int64(n)

		offset, err := ibFile.Seek(0, 1)
		if err != nil {
			return err
		}

		if cnt%sparseDeg == 0 {
			offset -= ib.StartOffset
			offset -= recSize // Start of the record

			sr := SummaryRecord{
				Key:    key,
				Offset: offset,
			}
			bytes = append(bytes, sb.writeRecord(sr)...)

			if startKey == nil { // First key
				startKey = key
			}

			offset += recSize
		}

		// Last key
		endKey = key

		if offset >= ib.StartOffset+ib.Size {
			break
		}
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
	err = util.WriteUvarint(file, uint64(len(startKey)))
	if err != nil {
		return err
	}
	_, err = file.Write(startKey)
	if err != nil {
		return err
	}

	// End Key
	err = util.WriteUvarint(file, uint64(len(endKey)))
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

	// Set the size of the summary block
	sb.Size, err = file.Seek(0, 1)
	if err != nil {
		return err
	}
	sb.Size -= sb.StartOffset

	return nil
}

// CreateFromIndexRecords creates a summary block from the given index records and writes it to disk.
// The sparseDeg parameter determines how many index records are skipped between each summary record.
// It also sets the size of the summary block.
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
		offset += int64(rec.sizeOnDisk())
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
	err = util.WriteUvarint(file, uint64(len(sumRecs[0].Key)))
	if err != nil {
		return err
	}

	_, err = file.Write(sumRecs[0].Key)
	if err != nil {
		return err
	}

	// End Key
	err = util.WriteUvarint(file, uint64(len(recs[len(recs)-1].Key)))
	if err != nil {
		return err
	}

	_, err = file.Write(recs[len(recs)-1].Key)
	if err != nil {
		return err
	}

	// Summary Records
	_, err = file.Write(bytes)
	if err != nil {
		return err
	}

	// Set the size of the summary block
	sb.Size, err = file.Seek(0, 1)
	if err != nil {
		return err
	}
	sb.Size -= sb.StartOffset

	return nil
}

// writeRecord writes a single SummaryRecord to a byte slice.
func (sb *SummaryBlock) writeRecord(sr SummaryRecord) []byte {
	bytes := make([]byte, 0)

	keySize := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(keySize, uint64(len(sr.Key)))
	bytes = append(bytes, keySize[:n]...)

	bytes = append(bytes, sr.Key...)

	offset := make([]byte, binary.MaxVarintLen64)
	_ = binary.PutUvarint(keySize, uint64(len(sr.Key)))
	bytes = append(bytes, offset[:n]...)

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
