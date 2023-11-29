package memtable

import (
	"log"
	"nasp-project/model"
	"nasp-project/structures/b_tree"
	"nasp-project/structures/hash_map"
	"nasp-project/structures/skip_list"
	"nasp-project/util"
)

// TODO: More instances implementation

type memtableStructure interface {
	// Add record to structure. Returns error if structure is full.
	Add(record *model.Record) error
	// Delete record from structure. Returns error if key does not exist.
	Delete(key []byte) error
	// Get key from structure. Return error if key does not exist.
	Get(key []byte) (*model.Record, error)
	// Flush returns sorted records.
	Flush() []*model.Record
}

type Memtable struct {
	structure memtableStructure
	readonly  bool
}

// NewMemtable creates an instance of Memtable. Creates Skip List if structure is invalid.
func NewMemtable(config *util.MemtableConfig) *Memtable {
	structure := config.Structure

	switch structure {
	case "BTree":
		return &Memtable{
			structure: b_tree.NewBTree(config.BTree.MinSize),
		}
	case "SkipList":
		return &Memtable{
			structure: skip_list.NewSkipList(uint32(config.MaxSize), uint32(config.SkipList.MaxHeight)),
		}
	case "HashMap":
		return &Memtable{
			structure: hash_map.NewHashMap(uint32(config.MaxSize)),
		}
	default:
		log.Print("warning: The memtable structure is invalid. The default structure (SkipList) will be used.")
		structure = "SkipList"
		return &Memtable{
			structure: skip_list.NewSkipList(uint32(config.MaxSize), uint32(config.SkipList.MaxHeight)),
		}
	}
}

func (mt *Memtable) Add(record *model.Record) error {
	return mt.structure.Add(record)
}

func (mt *Memtable) Delete(key []byte) error {
	return mt.structure.Delete(key)
}

func (mt *Memtable) Flush() []*model.Record {
	return mt.structure.Flush()
}

func (mt *Memtable) Get(key []byte) (*model.Record, error) {
	return mt.structure.Get(key)
}
