package mem_table

import (
	"log"
	"nasp-project/structures/b_tree"
	"nasp-project/structures/hash_map"
	"nasp-project/structures/skip_list"
	"nasp-project/util"
)

type DataRecord struct {
	Tombstone bool
	Key       []byte
	Value     []byte
	Timestamp int64
}

type MemTableStructure interface {
	Add(record *DataRecord) error
	Delete(key []byte) error
	Get(key []byte) (*DataRecord, error)
	Flush() []*DataRecord
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

func (mt *MemTable) Add(record *DataRecord) error {
	return mt.structure.Add(record)
}

func (mt *MemTable) Delete(key []byte) error {
	return mt.structure.Delete(key)
}

func (mt *MemTable) Flush() []*DataRecord {
	return mt.structure.Flush()
}
