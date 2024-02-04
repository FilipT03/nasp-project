package util

import (
	"encoding/binary"
	"os"
)

// WriteUvarint writes encodes num using variable-length encoding and writes it to file.
func WriteUvarint(file *os.File, num uint64) error {
	bytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(bytes, num)
	_, err := file.Write(bytes[:n])
	if err != nil {
		return err
	}
	return nil
}

// WriteUvarintLen writes encodes num using variable-length encoding, writes it to file
// and returns the number of bytes written to file.
func WriteUvarintLen(file *os.File, num uint64) (int, error) {
	bytes := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(bytes, num)
	n, err := file.Write(bytes[:n])
	if err != nil {
		return n, err
	}
	return n, nil
}

// ReadUvarint reads variable-length encoded number from file.
// The file pointer is set to the end of the read value bytes.
func ReadUvarint(file *os.File) (uint64, error) {
	bytes := make([]byte, binary.MaxVarintLen64)
	k, err := file.Read(bytes)
	if err != nil {
		return 0, err
	}

	val, n := binary.Uvarint(bytes)

	_, err = file.Seek(int64(n-k), 1)
	if err != nil {
		return 0, err
	}
	return val, nil
}

// ReadUvarintLen reads variable-length encoded number from file
// and returns the decoded number and the number of bytes read.
// The file pointer is set to the end of the read value bytes.
func ReadUvarintLen(file *os.File) (uint64, int, error) {
	bytes := make([]byte, binary.MaxVarintLen64)
	k, err := file.Read(bytes)
	if err != nil {
		return 0, 0, err
	}

	val, n := binary.Uvarint(bytes)

	_, err = file.Seek(int64(n-k), 1)
	if err != nil {
		return 0, 0, err
	}

	return val, n, nil
}
