package sstable

import (
	"encoding/binary"
	"nasp-project/structures/bloom_filter"
	"nasp-project/util"
	"os"
)

// FilterBlock represents a filter block in an SSTable
type FilterBlock struct {
	util.BinaryFile
	Filter *bloom_filter.BloomFilter // Lazy loaded filter
}

// HasLoaded returns true if the filter has been loaded into memory.
func (fb *FilterBlock) HasLoaded() bool {
	return fb.Filter != nil
}

// Load reads the filter block from disk and loads it into memory.
func (fb *FilterBlock) Load() error {
	file, err := os.Open(fb.Filename)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Seek(fb.StartOffset, 0)
	if err != nil {
		return err
	}
	bytes := make([]byte, fb.Size)
	_, err = file.Read(bytes)
	if err != nil {
		return err
	}
	fb.Filter = bloom_filter.Deserialize(bytes)
	return nil
}

// CreateFilter creates a filter from the given keys.
func (fb *FilterBlock) CreateFilter(keys [][]byte, p float64) {
	fb.Filter = bloom_filter.NewBloomFilter(uint(len(keys)), p)
	for _, key := range keys {
		fb.Filter.Add(key)
	}
}

func (fb *FilterBlock) CreateFromDataBlock(n uint, p float64, db *DataBlock) error {
	fb.Filter = bloom_filter.NewBloomFilter(n, p)

	file, err := os.Open(db.Filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Seek(db.StartOffset, 0)
	if err != nil {
		return err
	}

	for {
		offset, err := file.Seek(12, 1)
		if err != nil {
			return err
		}
		if offset >= db.Size {
			break
		}

		tombstone := make([]byte, 1)
		rl, err := file.Read(tombstone)
		if rl != 1 {
			break
		}
		if err != nil {
			return err
		}

		keySizeBytes := make([]byte, 8)
		rl, err = file.Read(keySizeBytes)
		if rl != 8 {
			break
		}
		if err != nil {
			return err
		}
		keySize := binary.LittleEndian.Uint64(keySizeBytes)

		var valueSize uint64 = 0
		if tombstone[0] == 0 {
			valueSizeBytes := make([]byte, 8)
			rl, err = file.Read(valueSizeBytes)
			if rl != 8 {
				break
			}
			if err != nil {
				return err
			}
			valueSize = binary.LittleEndian.Uint64(valueSizeBytes)
		}

		key := make([]byte, keySize)
		rl, err = file.Read(key)
		if rl != len(key) {
			break
		}
		if err != nil {
			return err
		}

		_, err = file.Seek(int64(valueSize), 1)
		if err != nil {
			return err
		}

		fb.Filter.Add(key)
	}

	return nil
}

// Write writes the filter block to disk.
// It also sets the size of the filter block.
func (fb *FilterBlock) Write() error {
	file, err := os.OpenFile(fb.Filename, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Seek(fb.StartOffset, 0)
	if err != nil {
		return err
	}
	_, err = file.Write(fb.Filter.Serialize())
	if err != nil {
		return err
	}

	fb.Size, err = file.Seek(0, 1)
	if err != nil {
		return err
	}
	fb.Size -= fb.StartOffset

	return nil
}
