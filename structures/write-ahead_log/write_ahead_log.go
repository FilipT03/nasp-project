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

	WALPath     = "wal" + string(os.PathSeparator)
	NumberStart = 4
	NumberEnd   = 8
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
	buffer         []*Record
	bufferSize     uint32
	segmentSize    uint64
	latestFileName string
}

func NewWAL(bufferSize uint32, segmentSize uint64) (*WAL, error) {
	dirEntries, err := os.ReadDir(WALPath)
	if os.IsNotExist(err) {
		err := os.Mkdir(WALPath, os.ModeDir)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	var latestFileName = ""
	if len(dirEntries) == 0 {
		f, err := os.OpenFile(WALPath+"wal_0001.log", os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		err = f.Truncate(HeaderSize)
		if err != nil {
			return nil, err
		}
		_, err = f.Write(binary.LittleEndian.AppendUint64(make([]byte, 0), HeaderSize))
		if err != nil {
			return nil, err
		}
		err = f.Close()
		if err != nil {
			return nil, err
		}
		latestFileName = "wal_0001.log"
	} else {
		latestFileName = dirEntries[len(dirEntries)-1].Name()
	}
	return &WAL{
		buffer:         make([]*Record, 0),
		bufferSize:     bufferSize,
		segmentSize:    segmentSize,
		latestFileName: latestFileName,
	}, nil
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

func (wal *WAL) incrementWALFileName() error {
	number, err := strconv.Atoi(wal.latestFileName[NumberStart:NumberEnd])
	if err != nil {
		return err
	}
	stringNumber := strconv.Itoa(number + 1)
	missing := (NumberEnd - NumberStart) - len(stringNumber)
	for missing > 0 {
		stringNumber = "0" + stringNumber
		missing -= 1
	}
	wal.latestFileName = wal.latestFileName[:NumberStart] + stringNumber + wal.latestFileName[NumberEnd:]
	return nil
}

func (wal *WAL) writeBuffer() error {
	toWrite := make([]byte, 0)

	f, err := os.OpenFile(WALPath+wal.latestFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	fStat, _ := f.Stat()
	fSize := fStat.Size()
	if uint64(fSize) == wal.segmentSize {
		err = f.Close()
		if err != nil {
			return err
		}
		err = wal.incrementWALFileName()
		if err != nil {
			return err
		}
		f, err := os.OpenFile(WALPath+wal.latestFileName, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		fStat, _ = f.Stat()
		fSize = fStat.Size()
	}
	defer f.Close()
	if fSize == 0 {
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
			for uint64(len(combinedSlice))-offset > (wal.segmentSize - HeaderSize) {
				var err error
				err = wal.writeSlice(nextHeader, combinedSlice[offset:offset+(wal.segmentSize-uint64(fSize))], WALPath+wal.latestFileName)
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
				err := wal.writeSlice(nextHeader, toWrite, WALPath+wal.latestFileName)
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
		err := wal.writeSlice(nextHeader, toWrite, WALPath+wal.latestFileName)
		if err != nil {
			return err
		}
	}
	// the latest file shouldn't be completely filled
	f, err = os.OpenFile(WALPath+wal.latestFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	fStat, _ = f.Stat()
	fSize = fStat.Size()
	if uint64(fSize) == wal.segmentSize {
		err = wal.incrementWALFileName()
		if err != nil {
			return err
		}
	}
	return nil
}

// writeSlice writes the byte array to the file of path. Returns error if data doesn't fit
func (wal *WAL) writeSlice(remainderSize uint64, slice []byte, path string) error {
	var returnError error = nil

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

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
	defer func(mmapFile *mmap.MMap) {
		err := mmapFile.Unmap()
		if err != nil {
			returnError = err
		}
	}(&mmapFile) // Unmap will flush the map before unmapping
	if err != nil {
		return err
	}

	if fileSize == 0 {
		binary.LittleEndian.PutUint64(mmapFile[0:HeaderSize], HeaderSize+remainderSize)
		copy(mmapFile[HeaderSize:], slice)
	} else {
		copy(mmapFile[fileSize:], slice)
	}
	if returnError != nil {
		return returnError
	}
	return nil
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
			f.Close()
			continue
		}

		mmapFile, err := mmap.Map(f, mmap.RDWR, 0)
		if err != nil {
			return nil, err
		}

		header := binary.LittleEndian.Uint64(mmapFile[0:HeaderSize])
		fmt.Println("header: ", header)
		if remainderSlice != nil { // we need to combine the end of the last file and start of this one
			record, err := wal.readRecordFromSlice(0, append(remainderSlice, mmapFile[HeaderSize:header]...))
			if err != nil {
				return nil, err
			}
			if record == nil { // the record is in more than two files
				remainderSlice = append(remainderSlice, mmapFile[HeaderSize:]...)
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

		err = mmapFile.Unmap()
		if err != nil {
			return nil, err
		}
		err = f.Close()
		if err != nil {
			return nil, err
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

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func (wal *WAL) Test() {
	arrayT := make([]byte, 0, 90)
	for i := 0; i < 90; i++ {
		arrayT = append(arrayT, byte('t'))
	}
	arrayP := make([]byte, 0, 98)
	for i := 0; i < 90; i++ {
		arrayP = append(arrayP, byte('p'))
	}
	//arrayP := append(arrayT, "tttttttt"...)
	//record120 := createRecord("T", arrayT, false)
	//record128 := createRecord("P", append(arrayT, "tttttttt"...), false)
	//fmt.Println(KeyStart + record120.KeySize + record120.ValueSize)
	//fmt.Println(KeyStart + record128.KeySize + record128.ValueSize)
	//fmt.Println(array100)
	wal.PutCommit("T", arrayT)
	wal.PutCommit("P", arrayP)
	err := wal.writeBuffer()
	if err != nil {
		panic(err)
	}
	records, err := wal.GetAllRecords()
	if err != nil {
		panic(err.Error())
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
