package write_ahead_log

import (
	"encoding/binary"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"os"
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

func (wal *WAL) PutCommit(Key string, Value []byte) {
	newRecord := &Record{
		CRC:       CRC32(Value),              //Generate CRC
		Timestamp: uint64(time.Now().Unix()), //Get current time
		Tombstone: false,
		KeySize:   uint64(len(Key)),
		ValueSize: uint64(len(Value)),
		Key:       Key,
		Value:     Value,
	}
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
	// first we create a []byte from records
	newData := make([]byte, 0)
	for _, element := range wal.buffer {
		newData = append(newData, wal.recordToByteArray(element)...)
	}

	f, err := os.OpenFile("wal_test.log", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer os.Remove("file.txt")
	defer f.Close()

	fi, _ := f.Stat()
	oldSize := fi.Size()
	err = f.Truncate(fi.Size() + int64(len(newData)))
	if err != nil {
		return err
	}

	mmapFile, err := mmap.Map(f, mmap.RDWR, 0)
	defer mmapFile.Unmap() // Unmap will flush the map before unmapping
	if err != nil {
		return err
	}

	copy(mmapFile[oldSize:], newData)

	return nil
}

func (wal *WAL) readRecord(path string, offset uint64) (*Record, error) {
	result := &Record{}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	defer os.Remove("file.txt")
	defer f.Close()

	mmapFile, err := mmap.Map(f, mmap.RDWR, 0)
	defer mmapFile.Unmap() // Unmap will flush the map before unmapping
	if err != nil {
		return nil, err
	}

	result.CRC = binary.LittleEndian.Uint32(mmapFile[offset+CrcStart : offset+TimestampStart])
	result.Timestamp = binary.LittleEndian.Uint64(mmapFile[offset+TimestampStart : offset+TombstoneStart])
	if mmapFile[offset+TombstoneStart] == 0 {
		result.Tombstone = false
	} else {
		result.Tombstone = true
	}
	result.KeySize = binary.LittleEndian.Uint64(mmapFile[offset+KeySizeStart : offset+ValueSizeStart])
	result.ValueSize = binary.LittleEndian.Uint64(mmapFile[offset+ValueSizeStart : offset+KeyStart])
	result.Key = string(mmapFile[offset+KeyStart : (offset + KeyStart + result.KeySize)])
	result.Value = make([]byte, result.ValueSize)
	copy(result.Value, mmapFile[(offset+KeyStart+result.KeySize):(offset+KeyStart+result.KeySize+result.ValueSize)])
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
	result = append(result, make([]byte, wal.segmentSize-uint64(len(result)))...)

	return result
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
	//wal.writeBuffer()
	res, _ := wal.readRecord("wal_test.log", wal.segmentSize)
	printRecord(res)
	res, _ = wal.readRecord("wal_test.log", wal.segmentSize*3)
	printRecord(res)
}
