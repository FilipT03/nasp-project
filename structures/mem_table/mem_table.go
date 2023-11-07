package mem_table

import (
	"nasp-project/structures/b_tree"
	"nasp-project/util"
)

type MemTableStructure interface {
	Add(key string, value []byte)
	Delete(key string) error
	Get(key string) ([]byte, error)
}

type MemTable struct {
	Structure MemTableStructure
	MaxSize   int
}

func NewMemTable() *MemTable {
	config := util.GetConfig()
	maxSize := config.MemTable.MaxSize
	structure := config.MemTable.Structure

	switch structure {
	case "BTree":
		return &MemTable{
			Structure: b_tree.NewBTree(config.MemTable.BTree.MinSize),
			MaxSize:   maxSize,
		}
	}

	return nil
}

func (mt *MemTable) Add(key string, value []byte) error {
	return nil
}

func (mt *MemTable) Delete(key string) error {
	return nil
}

func (mt *MemTable) Flush() error {
	return nil
}
