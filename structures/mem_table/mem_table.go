package mem_table

import (
	"log"
	"nasp-project/structures/b_tree"
	"nasp-project/structures/hash_map"
	"nasp-project/structures/skip_list"
	"nasp-project/util"
)

type MemTableStructure interface {
	// Add record to structure. Returns error if structure is full.
	Add(record *util.DataRecord) error
	// Delete record from structure. Returns error if key does not exist.
	Delete(key []byte) error
	// Get key from structure. Return error if key does not exist.
	Get(key []byte) (*util.DataRecord, error)
	// Flush returns sorted records and deletes table.
	Flush() []*util.DataRecord
}

type MemTable struct {
	structure MemTableStructure
}

// NewMemTable creates an instance of MemTable. Creates Skip List if structure is not defined.
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
