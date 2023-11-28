package sstable

type Block struct {
	Filename    string // Where the block is stored
	StartOffset int64  // Where the block starts in the file (in bytes)
	Size        int64  // Size of the block (in bytes)
}
