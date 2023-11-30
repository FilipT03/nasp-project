package write_ahead_log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"os"
	"strconv"
	"time"
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

	WALPath = "wal" + string(os.PathSeparator)
)

type Record struct {
	CRC       uint32
	Timestamp uint64
	Tombstone bool
	KeySize   uint64
	ValueSize uint64
	Key       string
	Value     []byte
}

type WAL struct {
	buffer      []*Record
	bufferSize  uint32
	segmentSize uint64
}

func NewWAL(bufferSize uint32, segmentSize uint64) *WAL {
	return &WAL{
		buffer:      make([]*Record, 0),
		bufferSize:  bufferSize,
		segmentSize: segmentSize,
	}
}

func (wal *WAL) PutCommit(key string, value []byte) {
	newRecord := createRecord(key, value, false)
	wal.commitRecord(newRecord)
}
func (wal *WAL) DeleteCommit(key string, value []byte) {
	newRecord := createRecord(key, value, true)
	wal.commitRecord(newRecord)
}

func (wal *WAL) commitRecord(record *Record) {
	wal.buffer = append(wal.buffer, record)
	if uint32(len(wal.buffer)) >= wal.bufferSize {
		err := wal.writeBuffer()
		if err != nil {
			wal.buffer = make([]*Record, 0)
		}
	}
}

func (wal *WAL) writeBuffer() error {
	var returnError error
	// first we create a []byte from records
	newData := make([]byte, 0)
	for _, element := range wal.buffer {
		newData = append(newData, wal.recordToByteArray(element)...)
	}

	f, err := os.OpenFile("wal"+string(os.PathSeparator)+"wal_test.log", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer func() {
		err := os.Remove("file.txt")
		if err != nil {
			returnError = err
		}
	}() // os.Remove("file.txt")
	defer f.Close()

	fi, _ := f.Stat()
	oldSize := fi.Size()
	if oldSize == 0 {
		err = f.Truncate(8 + int64(len(newData)))
	} else {
		err = f.Truncate(fi.Size() + int64(len(newData)))
	}
	if err != nil {
		return err
	}

	mmapFile, err := mmap.Map(f, mmap.RDWR, 0)
	defer func(mmapFile *mmap.MMap) {
		err := mmapFile.Unmap()
		if err != nil {
			returnError = err
		}
	}(&mmapFile) // Unmap will flush the map before unmapping
	if err != nil {
		return err
	}

	if oldSize == 0 {
		binary.LittleEndian.PutUint64(mmapFile[0:8], 8)
		copy(mmapFile[8:], newData)
	} else {
		copy(mmapFile[oldSize:], newData)
	}

	return returnError
}

func (wal *WAL) GetAllRecords() ([]*Record, error) {
	var returnError error = nil
	dirEntries, err := os.ReadDir(WALPath)
	if err != nil {
		return nil, err
	}
	result := make([]*Record, 0)
	var remainderSlice []byte = nil
	for _, entry := range dirEntries {
		path := WALPath + entry.Name()

		f, err := os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		fi, _ := f.Stat()
		fSize := fi.Size()
		if fSize <= HeaderSize {
			continue
		}

		mmapFile, err := mmap.Map(f, mmap.RDWR, 0) // ERROR PRI MAPIRANJU
		if err != nil {
			return nil, err
		}

		header := binary.LittleEndian.Uint64(mmapFile[0:HeaderSize])
		if remainderSlice != nil { // we need to combine the end of the last file and start of this one
			record, err := wal.readRecordFromSlice(0, append(remainderSlice, mmapFile...))
			if err != nil {
				return nil, err
			}
			if record == nil { // the record is in more than two files
				remainderSlice = append(remainderSlice, mmapFile...)
				break
			} else {
				result = append(result, record)
				remainderSlice = nil
			}
		}
		for offset := header; offset < uint64(fSize); {
			record, err := wal.readRecordFromSlice(offset, mmapFile)
			if err != nil {
				return nil, err
			}
			if record == nil { // we reached end of the file
				remainderLength := uint64(len(mmapFile)) - offset
				remainderSlice = make([]byte, remainderLength)
				copy(remainderSlice, mmapFile[offset:])
				break
			} else {
				result = append(result, record)
				offset += KeyStart + record.KeySize + record.ValueSize
			}
		}

		err = f.Close()
		if err != nil {
			return nil, err
		}
		err = mmapFile.Unmap()
		if err != nil {
			returnError = err
		}
	}
	return result, returnError
}

// readRecord reads record from slice with offset. Returns the read record and error if any occurred. Returns nil record
// if it can't read the whole record from the slice.
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

	if CRC32(result.Value) != result.CRC {
		return nil, errors.New("failed to read record of offset" + strconv.FormatUint(offset, 10) + ": CRCs don't match")
	}
	return result, nil
}

func (wal *WAL) recordToByteArray(record *Record) []byte {
	result := make([]byte, 0)
	result = binary.LittleEndian.AppendUint32(result, CRC32(record.Value))
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

func createRecord(key string, value []byte, tombstone bool) *Record {
	return &Record{
		CRC:       CRC32(value),              //Generate CRC
		Timestamp: uint64(time.Now().Unix()), //Get current time
		Tombstone: tombstone,
		KeySize:   uint64(len(key)),
		ValueSize: uint64(len(value)),
		Key:       key,
		Value:     value,
	}
}

func printRecord(record *Record) {
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

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (wal *WAL) Test() {
	wal.PutCommit("Test", []byte("First value"))
	wal.PutCommit("Test2", []byte("Second value"))
	err := wal.writeBuffer()
	if err != nil {
		panic(err)
	}
	records, err := wal.GetAllRecords()
	if err != nil {
		print(err.Error())
	}
	for _, record := range records {
		printRecord(record)
	}
	/*res, err := wal.readRecord("wal_test.log", wal.segmentSize)
	print(err)
	printRecord(res)
	res, err = wal.readRecord("wal_test.log", wal.segmentSize*3)
	print(err)
	printRecord(res)*/
}
