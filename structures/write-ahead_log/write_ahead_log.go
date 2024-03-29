package write_ahead_log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"nasp-project/model"
	"nasp-project/util"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/edsrzf/mmap-go"
)

/*
  +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
  |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
  +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
  CRC = 32bit hash computed over the payload using CRC
  Timestamp = Timestamp of the operation in seconds
  Tombstone = If this record was deleted and has a value
  Key Size = Length of the Key data
  Value Size = Length of the Value data
  Key = Key data
  Value = Value data

  Each file starts with a header of 8 bytes storing start of the first whole record in that segment
  in bytes, from the beginning of the file.
*/

const (
	CrcSize       = 4
	TimestampSize = 8
	TombstoneSize = 1
	KeySizeSize   = 8
	ValueSizeSize = 8

	CrcStart       = 0
	TimestampStart = CrcStart + CrcSize
	TombstoneStart = TimestampStart + TimestampSize
	KeySizeStart   = TombstoneStart + TombstoneSize
	ValueSizeStart = KeySizeStart + KeySizeSize
	KeyStart       = ValueSizeStart + ValueSizeSize

	HeaderSize = 8

	NumberStart = 4
	NumberEnd   = 12

	FileIndexSize  = 4
	ByteOffsetSize = 8

	FileIndexStart       = 0
	ByteOffsetStart      = FileIndexStart + FileIndexSize
	MemtableIndexingSize = FileIndexSize + ByteOffsetSize
)

// Record structure for WAL commits
type Record struct {
	CRC       uint32
	Timestamp uint64
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

// WAL - Write ahead log
type WAL struct {
	buffer               []*Record
	bufferSize           int
	segmentSize          uint64
	walFolderPath        string
	logsPath             string
	memtableIndexingPath string
	latestFileName       string
}

// NewWAL is constructor for the Write ahead log.
func NewWAL(walConfig *util.WALConfig, memtableInstances int) (*WAL, error) {
	logsPath := walConfig.WALFolderPath + string(os.PathSeparator) + "logs" + string(os.PathSeparator)
	memtableIndexingPath := walConfig.WALFolderPath + string(os.PathSeparator) + "memtable_indexing.bin"

	dirEntries, err := os.ReadDir(logsPath)
	if os.IsNotExist(err) {
		err := os.MkdirAll(logsPath, 0777)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	var latestFileName = ""
	emptyWal := false
	if len(dirEntries) == 0 { // create empty first log
		emptyWal = true
		latestFileName = "wal_" + strings.Repeat("0", (NumberEnd-NumberStart)-1) + "1.log"
		f, err := os.OpenFile(logsPath+latestFileName, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		defer func(f *os.File) {
			_ = f.Close()
		}(f)
		err = f.Truncate(HeaderSize)
		if err != nil {
			return nil, err
		}
		_, err = f.Write(binary.LittleEndian.AppendUint64(make([]byte, 0), HeaderSize))
		if err != nil {
			return nil, err
		}
	} else { // set latestFileName to the latest file
		latestFileName = dirEntries[len(dirEntries)-1].Name()
	}
	f, err := os.OpenFile(memtableIndexingPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)
	// Deleting old memtable indexes since they will be recalculated
	err = f.Truncate(int64(MemtableIndexingSize))
	if err != nil {
		return nil, err
	}
	err = f.Truncate(int64(MemtableIndexingSize * (memtableInstances + 1)))
	if err != nil {
		return nil, err
	}
	if emptyWal { // if there were no logs, we need set the start to the new log
		_, _ = f.Seek(0, 0)
		byteS := make([]byte, 0)
		byteS = binary.LittleEndian.AppendUint32(byteS, 1)
		byteS = binary.LittleEndian.AppendUint64(byteS, HeaderSize)
		_, _ = f.Write(byteS)
	}

	return &WAL{
		buffer:               make([]*Record, 0),
		bufferSize:           walConfig.BufferSize,
		segmentSize:          walConfig.SegmentSize,
		walFolderPath:        walConfig.WALFolderPath,
		logsPath:             logsPath,
		memtableIndexingPath: memtableIndexingPath,
		latestFileName:       latestFileName,
	}, nil
}

// PutCommit adds put commit to the WAL buffer.
func (wal *WAL) PutCommit(key string, value []byte) error {
	newRecord := createRecord(key, value, false)
	err := wal.commitRecord(newRecord)
	if err != nil {
		return err
	}
	return nil
}

// DeleteCommit adds delete commit to the WAL buffer.
func (wal *WAL) DeleteCommit(key string, value []byte) error {
	newRecord := createRecord(key, value, true)
	err := wal.commitRecord(newRecord)
	if err != nil {
		return err
	}
	return nil
}

// commitRecord adds record to the buffer, and calls writeBuffer if it's full.
func (wal *WAL) commitRecord(record *Record) error {
	wal.buffer = append(wal.buffer, record)
	if len(wal.buffer) >= wal.bufferSize {
		err := wal.writeBuffer()
		if err != nil {
			return err
		}
		wal.buffer = make([]*Record, 0)
	}
	return nil
}

// EmptyBuffer writes everything in the buffer and clears it.
func (wal *WAL) EmptyBuffer() error {
	err := wal.writeBuffer()
	if err != nil {
		return err
	}
	wal.buffer = make([]*Record, 0)
	return nil
}

// FlushedMemtable is called by a memtable.Memtable after it was successfully flushed into the sstable.SSTable.
// Deletes old logs that were written in the SSTable during the flushing.
func (wal *WAL) FlushedMemtable(memtableIndex int) error {
	err := wal.writeBuffer()
	if err == nil {
		wal.buffer = make([]*Record, 0)
	} else {
		return err
	}
	f, err := os.OpenFile(wal.memtableIndexingPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)
	_, err = f.Seek(int64(MemtableIndexingSize*(memtableIndex+1)), 0)
	if err != nil {
		return err
	}
	byteS := make([]byte, MemtableIndexingSize)
	_, err = f.Read(byteS)
	if err != nil {
		return err
	}
	fileIndex := binary.LittleEndian.Uint32(byteS[FileIndexStart : FileIndexStart+FileIndexSize])
	byteOffset := binary.LittleEndian.Uint64(byteS[ByteOffsetStart : ByteOffsetStart+ByteOffsetSize])

	if fileIndex != 0 && byteOffset != 0 { // Don't delete files if this memtable is not initialized yet
		// Deleting the unneeded logs
		dirEntries, err := os.ReadDir(wal.logsPath)
		if err != nil {
			return err
		}
		stringNumber := strconv.Itoa(int(fileIndex))
		missing := (NumberEnd - NumberStart) - len(stringNumber)
		for missing > 0 {
			stringNumber = "0" + stringNumber
			missing -= 1
		}
		endFile := "wal_" + stringNumber + ".log"
		for _, entry := range dirEntries {
			if entry.Name() == endFile {
				break
			}
			err := os.Remove(wal.logsPath + entry.Name())
			if err != nil {
				return err
			}
		}
	}

	// Updating the memtable indexing
	latestFile, err := os.OpenFile(wal.logsPath+wal.latestFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func(latestFile *os.File) {
		_ = latestFile.Close()
	}(latestFile)
	latestFileStat, err := latestFile.Stat()
	if err != nil {
		return err
	}
	latestFileSize := latestFileStat.Size()
	byteS = make([]byte, 0)
	newFileIndex, err := strconv.Atoi(wal.latestFileName[NumberStart:NumberEnd])
	var newByteOffset uint64
	if latestFileSize <= HeaderSize {
		newFileIndex--
		newByteOffset = wal.segmentSize
	} else {
		newByteOffset = uint64(latestFileSize)
	}
	if err != nil {
		return err
	}
	byteS = binary.LittleEndian.AppendUint32(byteS, uint32(newFileIndex))
	byteS = binary.LittleEndian.AppendUint64(byteS, newByteOffset)

	_, err = f.Seek(-int64(MemtableIndexingSize), 1)
	if err != nil {
		return err
	}
	_, err = f.Write(byteS)
	if err != nil {
		return err
	}

	if fileIndex != 0 && byteOffset != 0 { // Don't update start if this memtable wasn't initialized
		_, err = f.Seek(0, 0)
		if err != nil {
			return err
		}
		byteS = make([]byte, 0)
		byteS = binary.LittleEndian.AppendUint32(byteS, fileIndex)
		byteS = binary.LittleEndian.AppendUint64(byteS, byteOffset)
		_, err = f.Write(byteS)
		if err != nil {
			return err
		}
	}

	return nil
}

// incrementWALFileName increments WAL latestFileName by one.
func (wal *WAL) incrementWALFileName() error {
	number, err := strconv.Atoi(wal.latestFileName[NumberStart:NumberEnd])
	if err != nil {
		return err
	}
	stringNumber := strconv.Itoa(number + 1)
	missing := (NumberEnd - NumberStart) - len(stringNumber)
	stringNumber = strings.Repeat("0", missing) + stringNumber
	wal.latestFileName = wal.latestFileName[:NumberStart] + stringNumber + wal.latestFileName[NumberEnd:]
	return nil
}

// writeBuffer writes records from the buffer and creates new files when necessary. Doesn't clear the buffer.
func (wal *WAL) writeBuffer() error {
	toWrite := make([]byte, 0)

	f, err := os.OpenFile(wal.logsPath+wal.latestFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)
	fStat, _ := f.Stat()
	fSize := fStat.Size()
	if uint64(fSize) == wal.segmentSize {
		err = wal.incrementWALFileName()
		if err != nil {
			return err
		}
		f2, err := os.OpenFile(wal.logsPath+wal.latestFileName, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer func(f2 *os.File) {
			_ = f2.Close()
		}(f2)
		fSize = HeaderSize
	} else if fSize == 0 {
		// we will look at empty files as if they contained the header, because no file can be smaller than
		// the HeaderSize
		fSize = HeaderSize
	}

	var nextHeader uint64 = 0
	for _, element := range wal.buffer {
		nextSlice := wal.recordToByteArray(element)
		if uint64(len(toWrite))+uint64(len(nextSlice)) > (wal.segmentSize - uint64(fSize)) {
			var offset uint64 = 0
			combinedSlice := append(toWrite, nextSlice...)
			// while there is more to write that a single file can fit
			for uint64(len(combinedSlice))-offset > (wal.segmentSize - uint64(fSize)) {
				var err error
				err = wal.writeSlice(nextHeader, combinedSlice[offset:offset+(wal.segmentSize-uint64(fSize))], wal.logsPath+wal.latestFileName)
				offset += wal.segmentSize - uint64(fSize)
				fSize = HeaderSize
				// in case the loop continues, we need to prepare the nextHeader
				nextHeader = wal.segmentSize - HeaderSize
				if err != nil {
					return err
				}
				// incrementing the log number
				err = wal.incrementWALFileName()
				if err != nil {
					return err
				}
			}
			toWrite = combinedSlice[offset:]
			nextHeader = uint64(len(toWrite))
		} else {
			toWrite = append(toWrite, nextSlice...)
			if uint64(len(toWrite)) == (wal.segmentSize - uint64(fSize)) {
				err := wal.writeSlice(nextHeader, toWrite, wal.logsPath+wal.latestFileName)
				if err != nil {
					return err
				}
				fSize = HeaderSize
				nextHeader = 0
				err = wal.incrementWALFileName()
				if err != nil {
					return err
				}
				toWrite = make([]byte, 0)
			}
		}
	}
	if len(toWrite) > 0 {
		err := wal.writeSlice(nextHeader, toWrite, wal.logsPath+wal.latestFileName)
		if err != nil {
			return err
		}
	}
	// the latest file shouldn't be completely filled
	f3, err := os.OpenFile(wal.logsPath+wal.latestFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func(f3 *os.File) {
		_ = f3.Close()
	}(f3)
	fStat, _ = f3.Stat()
	fSize = fStat.Size()
	if uint64(fSize) == wal.segmentSize {
		err = wal.incrementWALFileName()
		if err != nil {
			return err
		}
	}
	return nil
}

// writeSlice writes the byte array to the file of path. Returns error if data doesn't fit.
func (wal *WAL) writeSlice(remainderSize uint64, slice []byte, path string) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()
	if fileSize == 0 && uint64(len(slice))+uint64(HeaderSize) > wal.segmentSize {
		return errors.New("failed to write slice, data would exceed the segment size")
	}
	if fileSize != 0 && uint64(len(slice))+uint64(fileSize) > wal.segmentSize {
		return errors.New("failed to write slice, data would exceed the segment size")
	}

	if fileSize == 0 {
		err := f.Truncate(int64(len(slice)) + int64(HeaderSize))
		if err != nil {
			return err
		}
	} else {
		err := f.Truncate(int64(len(slice)) + fileSize)
		if err != nil {
			return err
		}
	}

	mmapFile, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer func(mmapFile *mmap.MMap) {
		_ = mmapFile.Unmap()
	}(&mmapFile)

	if fileSize == 0 {
		binary.LittleEndian.PutUint64(mmapFile[0:HeaderSize], HeaderSize+remainderSize)
		copy(mmapFile[HeaderSize:], slice)
	} else {
		copy(mmapFile[fileSize:], slice)
	}
	return nil
}

// GetAllRecords reads records from WAL files and returns them with two additional slices, one for ending file index of
// that record and other for the byte offset.
func (wal *WAL) GetAllRecords() ([]*model.Record, []uint32, []uint64, error) {
	f, err := os.OpenFile(wal.memtableIndexingPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)
	fi, _ := f.Stat()
	fSize := fi.Size()
	byteS := make([]byte, fSize)
	_, _ = f.Read(byteS)
	startFileIndex := binary.LittleEndian.Uint32(byteS[0:FileIndexSize])
	startByteOffset := binary.LittleEndian.Uint64(byteS[FileIndexSize:MemtableIndexingSize])

	dirEntries, err := os.ReadDir(wal.logsPath)
	if err != nil {
		return nil, nil, nil, err
	}
	records := make([]*model.Record, 0)
	allFileIndexes := make([]uint32, 0)
	allByteOffsets := make([]uint64, 0)
	var remainderSlice []byte = nil

	oldestFileIndex, err := strconv.Atoi(dirEntries[0].Name()[NumberStart:NumberEnd])
	if err != nil {
		return nil, nil, nil, err
	}

	var toSkip int64
	if startFileIndex == 0 {
		toSkip = 0
	} else {
		toSkip = int64(startFileIndex) - int64(oldestFileIndex)
		if toSkip < 0 {
			return nil, nil, nil, errors.New("memtable indexing file referencing non existent files")
		}
	}

	first := true
	for _, entry := range dirEntries {
		if toSkip > 0 {
			toSkip--
			continue
		}
		currentFileIndex, err := strconv.Atoi(entry.Name()[NumberStart:NumberEnd])
		if err != nil {
			return nil, nil, nil, err
		}
		path := wal.logsPath + entry.Name()
		f, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return nil, nil, nil, err
		}

		fi, _ := f.Stat()
		fSize := fi.Size()
		if fSize <= HeaderSize {
			_ = f.Close()
			continue
		}

		mmapFile, err := mmap.Map(f, mmap.RDWR, 0)
		if err != nil {
			return nil, nil, nil, err
		}

		header := binary.LittleEndian.Uint64(mmapFile[0:HeaderSize])
		if first {
			first = false
			if startByteOffset > HeaderSize {
				header = startByteOffset
			}
		}
		//fmt.Println("header: ", header)
		if remainderSlice != nil { // we need to combine the end of the last file and start of this one
			record, err := wal.readRecordFromSlice(0, append(remainderSlice, mmapFile[HeaderSize:header]...))
			if err != nil {
				return nil, nil, nil, err
			}
			if record == nil { // the record is in more than two files
				remainderSlice = append(remainderSlice, mmapFile[HeaderSize:]...)
				continue
			} else {
				records = append(records, record.ToModelRecord())
				allFileIndexes = append(allFileIndexes, uint32(currentFileIndex))
				allByteOffsets = append(allByteOffsets, header)
				remainderSlice = nil
			}
		}
		for offset := header; offset < uint64(fSize); {
			record, err := wal.readRecordFromSlice(offset, mmapFile)
			if err != nil {
				return nil, nil, nil, err
			}
			if record == nil { // we reached end of the file
				remainderLength := uint64(len(mmapFile)) - offset
				remainderSlice = make([]byte, remainderLength)
				copy(remainderSlice, mmapFile[offset:])
				break
			} else {
				records = append(records, record.ToModelRecord())
				offset += KeyStart + record.KeySize + record.ValueSize
				allFileIndexes = append(allFileIndexes, uint32(currentFileIndex))
				allByteOffsets = append(allByteOffsets, offset)
			}
		}

		err = mmapFile.Unmap()
		if err != nil {
			return nil, nil, nil, err
		}
		err = f.Close()
		if err != nil {
			return nil, nil, nil, err
		}
	}
	return records, allFileIndexes, allByteOffsets, nil
}

// UpdateMemtableIndexing updates memtable indexing file with new values got from memtable.Memtable
func (wal *WAL) UpdateMemtableIndexing(fileIndexes []uint32, byteOffsets []uint64) error {
	f, err := os.OpenFile(wal.memtableIndexingPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)
	_, err = f.Seek(MemtableIndexingSize, 0)
	if err != nil {
		return err
	}

	byteS := make([]byte, 0)
	for i := 0; i < len(fileIndexes); i++ {
		byteS = binary.LittleEndian.AppendUint32(byteS, fileIndexes[i])
		byteS = binary.LittleEndian.AppendUint64(byteS, byteOffsets[i])
	}
	_, err = f.Write(byteS)
	if err != nil {
		return err
	}
	return nil
}

// readRecord reads Record from slice with offset. Returns the read Record and error if any occurred. Returns nil record
// if it can't read the whole Record from the slice.
func (wal *WAL) readRecordFromSlice(offset uint64, slice []byte) (*Record, error) {
	result := &Record{}

	if uint64(len(slice)) < offset+KeyStart {
		return nil, nil
	}
	result.KeySize = binary.LittleEndian.Uint64(slice[offset+KeySizeStart : offset+ValueSizeStart])
	result.ValueSize = binary.LittleEndian.Uint64(slice[offset+ValueSizeStart : offset+KeyStart])
	if uint64(len(slice)) < offset+KeyStart+result.KeySize+result.ValueSize {
		return nil, nil
	}

	result.CRC = binary.LittleEndian.Uint32(slice[offset+CrcStart : offset+TimestampStart])
	result.Timestamp = binary.LittleEndian.Uint64(slice[offset+TimestampStart : offset+TombstoneStart])
	if slice[offset+TombstoneStart] == 0 {
		result.Tombstone = false
	} else {
		result.Tombstone = true
	}
	result.Key = string(slice[offset+KeyStart : (offset + KeyStart + result.KeySize)])
	result.Value = make([]byte, result.ValueSize)
	copy(result.Value, slice[(offset+KeyStart+result.KeySize):(offset+KeyStart+result.KeySize+result.ValueSize)])

	if util.CRC32(result.Value) != result.CRC {
		return nil, errors.New("failed to read record of offset" + strconv.FormatUint(offset, 10) + ": CRCs don't match")
	}
	return result, nil
}

// recordToByteArray converts Record to byte array.
func (wal *WAL) recordToByteArray(record *Record) []byte {
	result := make([]byte, 0)
	result = binary.LittleEndian.AppendUint32(result, util.CRC32(record.Value))
	result = binary.LittleEndian.AppendUint64(result, record.Timestamp)
	if record.Tombstone {
		result = append(result, byte(1))
	} else {
		result = append(result, byte(0))
	}
	result = binary.LittleEndian.AppendUint64(result, record.KeySize)
	result = binary.LittleEndian.AppendUint64(result, record.ValueSize)
	result = append(result, record.Key...)
	result = append(result, record.Value...)
	//result = append(result, make([]byte, wal.recordSize-uint64(len(result)))...)

	return result
}

// createRecord constructs Record.
func createRecord(key string, value []byte, tombstone bool) *Record {
	return &Record{
		CRC:       util.CRC32(value),         //Generate CRC
		Timestamp: uint64(time.Now().Unix()), //Get current time
		Tombstone: tombstone,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
	}
}

// printRecord prints Record in a readable format.
func printRecord(record *Record) {
	if record == nil {
		print("nil")
		return
	}
	fmt.Printf("CRC: %d\n", record.CRC)
	fmt.Printf("Timestamp: %d\n", record.Timestamp)
	fmt.Printf("Tombstone: %t\n", record.Tombstone)
	fmt.Printf("KeySize: %d\n", record.KeySize)
	fmt.Printf("ValueSize: %d\n", record.ValueSize)
	fmt.Printf("Key: %s\n", record.Key)
	fmt.Print("Value: ")
	for _, b := range record.Value {
		fmt.Print(string(b))
		//fmt.Print(" ")
	}
	fmt.Println()
}

func (rec *Record) ToModelRecord() *model.Record {
	return &model.Record{
		Key:       []byte(rec.Key),
		Value:     rec.Value,
		Tombstone: rec.Tombstone,
		Timestamp: rec.Timestamp,
	}
}

func (rec *Record) Equals(other *Record, ignoreTimestamp bool) bool {
	return rec.CRC == other.CRC &&
		rec.Tombstone == other.Tombstone &&
		(rec.Timestamp == other.Timestamp || ignoreTimestamp) &&
		rec.KeySize == other.KeySize &&
		rec.Key == other.Key &&
		rec.ValueSize == other.ValueSize &&
		bytes.Equal(rec.Value, other.Value)
}

func (rec *Record) ToString() string {
	if rec == nil {
		return "nil"
	}
	elems := []string{
		fmt.Sprintf("CRC: %d", rec.CRC),
		fmt.Sprintf("Timestamp: %d", rec.Timestamp),
		fmt.Sprintf("Tombstone: %t", rec.Tombstone),
		fmt.Sprintf("KeySize: %d", rec.KeySize),
		fmt.Sprintf("ValueSize: %d", rec.ValueSize),
		fmt.Sprintf("Key: %s", rec.Key),
		fmt.Sprintf("Value: %s", string(rec.Value)),
	}
	return strings.Join(elems, "\n")
}
