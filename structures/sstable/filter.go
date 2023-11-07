package sstable

import (
	"nasp-project/structures/bloom_filter"
	"os"
)

// FilterBlock represents a filter block in an SSTable
type FilterBlock struct {
	Filename    string                    // Where the filter block is stored
	StartOffset int64                     // Where the filter block starts in the file (in bytes)
	Size        int64                     // Size of the filter block (in bytes)
	Filter      *bloom_filter.BloomFilter // Lazy loaded filter
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
