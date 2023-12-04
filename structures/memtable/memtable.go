package memtable

import (
	"log"
	"nasp-project/model"
	"nasp-project/structures/b_tree"
	"nasp-project/structures/hash_map"
	"nasp-project/structures/skip_list"
	"nasp-project/structures/sstable"
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
	Flush() []model.Record
}

type Memtable struct {
	structure memtableStructure
}

var memtables = struct {
	index     int
	maxTables int
	tables    []*Memtable
}{index: 0, maxTables: 0, tables: nil}

// NewMemtable creates an instance of Memtable. Creates Skip List if structure is invalid.
func NewMemtable(config *util.MemtableConfig) *Memtable {
	structure := config.Structure
	instances := config.Instances

	memtables.tables = make([]*Memtable, 0)
	memtables.maxTables = instances - 1

	switch structure {
	case "BTree":
		for i := 0; i < instances; i++ {
			memtables.tables = append(memtables.tables, &Memtable{
				structure: b_tree.NewBTree(config.BTree.MinSize),
			})
		}
		return memtables.tables[memtables.index]
	case "SkipList":
		for i := 0; i < instances; i++ {
			memtables.tables = append(memtables.tables, &Memtable{
				structure: skip_list.NewSkipList(uint32(config.MaxSize), uint32(config.SkipList.MaxHeight)),
			})
		}
		return memtables.tables[memtables.index]
	case "HashMap":
		for i := 0; i < instances; i++ {
			memtables.tables = append(memtables.tables, &Memtable{
				structure: hash_map.NewHashMap(uint32(config.MaxSize)),
			})
		}
		return memtables.tables[memtables.index]
	default:
		log.Print("warning: The memtable structure is invalid. The default structure (SkipList) will be used.")
		structure = "SkipList"
		for i := 0; i < instances; i++ {
			memtables.tables = append(memtables.tables, &Memtable{
				structure: skip_list.NewSkipList(uint32(config.MaxSize), uint32(config.SkipList.MaxHeight)),
			})
		}
		return memtables.tables[memtables.index]
	}
}

func flush() {
	for _, table := range memtables.tables {
		records := table.structure.Flush()
		_, err := sstable.CreateSSTable(records, util.GetConfig().SSTable)
		if err != nil {
			panic("error: could not flush table - " + err.Error())
		}
	}
}

func (mt *Memtable) Add(record *model.Record) {
	err := mt.structure.Add(record)
	if err != nil {
		if memtables.index == memtables.maxTables {
			flush()
			memtables.index = 0
			*mt = *memtables.tables[memtables.index]
			return
		}
		memtables.index++
		_ = memtables.tables[memtables.index].structure.Add(record)
		*mt = *memtables.tables[memtables.index]
	}
}

func (mt *Memtable) Delete(key []byte) error {
	return mt.structure.Delete(key)
}

func (mt *Memtable) Get(key []byte) (*model.Record, error) {
	return mt.structure.Get(key)
}
