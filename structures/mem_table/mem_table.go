package mem_table

import (
	"log/slog"
	"nasp-project/structures/b_tree"
	"nasp-project/structures/hash_map"
	"nasp-project/structures/skip_list"
	"nasp-project/util"
)

type MemTableStructure interface {
	Add(key string, value []byte) error
	Delete(key string) error
	Get(key string) ([]byte, error)
}

type MemTable struct {
	Structure MemTableStructure
}

func NewMemTable() *MemTable {
	config := util.GetConfig()
	structure := config.MemTable.Structure

	switch structure {
	case "BTree":
		return &MemTable{
			Structure: b_tree.NewBTree(config.MemTable.BTree.MinSize),
		}
	case "SkipList":
		return &MemTable{
			Structure: skip_list.NewSkipList(uint32(config.MemTable.MaxSize), uint32(config.MemTable.SkipList.MaxHeight)),
		}
	case "HashMap":
		return &MemTable{
			Structure: hash_map.NewHashMap(uint32(config.MemTable.MaxSize)),
		}
	default:
		slog.Warn("warning: The memory table structure is invalid. The default structure will be used (SkipList).")
		structure = "SkipList"
		return &MemTable{
			Structure: skip_list.NewSkipList(uint32(config.MemTable.MaxSize), uint32(config.MemTable.SkipList.MaxHeight)),
		}
	}
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
