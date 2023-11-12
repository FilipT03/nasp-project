package mem_table

import (
	"log"
	"nasp-project/structures/b_tree"
	"nasp-project/structures/hash_map"
	"nasp-project/structures/skip_list"
	"nasp-project/util"
)

type MemTableStructure interface {
	Add(record *util.DataRecord) error
	Delete(key []byte) error
	Get(key []byte) (*util.DataRecord, error)
	Flush() []*util.DataRecord
}

type MemTable struct {
	structure MemTableStructure
}

func NewMemTable() *MemTable {
	config := util.GetConfig()
	structure := config.MemTable.Structure

	switch structure {
	case "BTree":
		return &MemTable{
			structure: b_tree.NewBTree(config.MemTable.BTree.MinSize),
		}
	case "SkipList":
		return &MemTable{
			structure: skip_list.NewSkipList(uint32(config.MemTable.MaxSize), uint32(config.MemTable.SkipList.MaxHeight)),
		}
	case "HashMap":
		return &MemTable{
			structure: hash_map.NewHashMap(uint32(config.MemTable.MaxSize)),
		}
	default:
		log.Print("warning: The memory table structure is invalid. The default structure (SkipList) will be used.")
		structure = "SkipList"
		return &MemTable{
			structure: skip_list.NewSkipList(uint32(config.MemTable.MaxSize), uint32(config.MemTable.SkipList.MaxHeight)),
		}
	}
}

func (mt *MemTable) Add(record *util.DataRecord) error {
	return mt.structure.Add(record)
}

func (mt *MemTable) Delete(key []byte) error {
	return mt.structure.Delete(key)
}

func (mt *MemTable) Flush() []*util.DataRecord {
	return mt.structure.Flush()
}
