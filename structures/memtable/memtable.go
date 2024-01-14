package memtable

import (
	"errors"
	"log"
	"nasp-project/model"
	"nasp-project/structures/b_tree"
	"nasp-project/structures/hash_map"
	"nasp-project/structures/skip_list"
	"nasp-project/util"
)

type memtableStructure interface {
	Add(record *model.Record) error
	Delete(key []byte) error
	Get(key []byte) (*model.Record, error)
	Flush() []model.Record
	Clear()
	IsFull() bool
}

type Memtable struct {
	structure memtableStructure
}

var Memtables = struct {
	currentIndex int
	lastIndex    int
	maxTables    int
	tables       []*Memtable
}{currentIndex: 0, lastIndex: 0, maxTables: 0, tables: nil}

// CreateMemtables creates instances of Memtable.
// If the structure is invalid, it creates a Skip List.
func CreateMemtables(config *util.MemtableConfig) {
	structure := config.Structure
	instances := config.Instances

	Memtables.tables = make([]*Memtable, 0)
	Memtables.maxTables = instances

	switch structure {
	case "BTree":
		for i := 0; i < instances; i++ {
			Memtables.tables = append(Memtables.tables, &Memtable{
				structure: b_tree.NewBTree(config.BTree.MinSize),
			})
		}
	case "SkipList":
		for i := 0; i < instances; i++ {
			Memtables.tables = append(Memtables.tables, &Memtable{
				structure: skip_list.NewSkipList(uint32(config.MaxSize), uint32(config.SkipList.MaxHeight)),
			})
		}
	case "HashMap":
		for i := 0; i < instances; i++ {
			Memtables.tables = append(Memtables.tables, &Memtable{
				structure: hash_map.NewHashMap(uint32(config.MaxSize)),
			})
		}
	default:
		log.Print("warning: The memtable structure is invalid. The default structure (SkipList) will be used.")
		structure = "SkipList"
		for i := 0; i < instances; i++ {
			Memtables.tables = append(Memtables.tables, &Memtable{
				structure: skip_list.NewSkipList(uint32(config.MaxSize), uint32(config.SkipList.MaxHeight)),
			})
		}
	}
}

// Add a record to the structure. Automatically switches tables if the current one is full.
// Returns an error if all tables are full.
func Add(record *model.Record) error {
	mt := Memtables.tables[Memtables.currentIndex]
	if mt.structure.IsFull() {
		Memtables.currentIndex = (Memtables.currentIndex + 1) % Memtables.maxTables
		if Memtables.currentIndex == Memtables.lastIndex {
			return errors.New("memtables full")
		}
		mt = Memtables.tables[Memtables.currentIndex]
	}
	return mt.structure.Add(record)
}

// Clear deletes all memtables.
func Clear() {
	for _, table := range Memtables.tables {
		table.structure.Clear()
	}

	Memtables.currentIndex = 0
	Memtables.lastIndex = 0
	Memtables.maxTables = 0
	Memtables.tables = nil
}

// Delete record from structure. Returns error if key does not exist.
func Delete(key []byte) error {
	return Memtables.tables[Memtables.currentIndex].structure.Delete(key)
}

// Get key from structure. Return error if key does not exist.
func Get(key []byte) (*model.Record, error) {
	index := Memtables.currentIndex
	for {
		record, err := Memtables.tables[index].structure.Get(key)
		if err == nil {
			return record, nil
		}
		index = (index - 1) % Memtables.maxTables
		if index == Memtables.currentIndex {
			break
		}
	}
	return nil, errors.New("error: key '" + string(key) + "' not found in " + util.GetConfig().Memtable.Structure)
}

// IsFull returns true if all memtables are completely filled.
func IsFull() bool {
	return Memtables.tables[Memtables.currentIndex].structure.IsFull() &&
		(Memtables.currentIndex+1)%Memtables.maxTables == Memtables.lastIndex
}

// Flush returns all records from the last memtable, clears the memtable and rotates accordingly.
func Flush() []model.Record {
	records := Memtables.tables[Memtables.lastIndex].structure.Flush()
	Memtables.tables[Memtables.lastIndex].structure.Clear()
	Memtables.lastIndex = (Memtables.lastIndex + 1) % Memtables.maxTables
	return records
}
