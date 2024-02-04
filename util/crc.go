package util

import "hash/crc32"

// CRC32 calculates CRC checksum for the given byte array.
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
