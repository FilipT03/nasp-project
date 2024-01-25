package memtable

import (
	"bytes"
	"errors"
	"log"
	"math"
	"nasp-project/model"
	"nasp-project/structures/b_tree"
	"nasp-project/structures/hash_map"
	"nasp-project/structures/iterator"
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
	NewIterator() (iterator.Iterator, error)
}

type Memtable struct {
	structure memtableStructure
}

type Memtables struct {
	currentIndex int
	lastIndex    int
	maxTables    int
	tables       []*Memtable
}

// CreateMemtables creates instances of Memtable.
// If the structure is invalid, it creates a Skip List.
func CreateMemtables(config *util.MemtableConfig) *Memtables {
	structure := config.Structure
	instances := config.Instances

	memts := &Memtables{}

	memts.tables = make([]*Memtable, 0)
	memts.maxTables = instances

	switch structure {
	case "BTree":
		for i := 0; i < instances; i++ {
			memts.tables = append(memts.tables, &Memtable{
				structure: b_tree.NewBTree(config.BTree.MinSize),
			})
		}
	case "SkipList":
		for i := 0; i < instances; i++ {
			memts.tables = append(memts.tables, &Memtable{
				structure: skip_list.NewSkipList(uint32(config.MaxSize), uint32(config.SkipList.MaxHeight)),
			})
		}
	case "HashMap":
		for i := 0; i < instances; i++ {
			memts.tables = append(memts.tables, &Memtable{
				structure: hash_map.NewHashMap(uint32(config.MaxSize)),
			})
		}
	default:
		log.Print("warning: The memtable structure is invalid. The default structure (SkipList) will be used.")
		structure = "SkipList"
		for i := 0; i < instances; i++ {
			memts.tables = append(memts.tables, &Memtable{
				structure: skip_list.NewSkipList(uint32(config.MaxSize), uint32(config.SkipList.MaxHeight)),
			})
		}
	}

	return memts
}

// Add a record to the structure. Automatically switches tables if the current one is full.
// Returns an error if all tables are full.
func (mts *Memtables) Add(record *model.Record) error {
	mt := mts.tables[mts.currentIndex]
	if mt.structure.IsFull() {
		mts.currentIndex = (mts.currentIndex + 1) % mts.maxTables
		if mts.currentIndex == mts.lastIndex {
			return errors.New("memtables full")
		}
		mt = mts.tables[mts.currentIndex]
	}
	return mt.structure.Add(record)
}

// Clear deletes all memtables.
func (mts *Memtables) Clear() {
	for _, table := range mts.tables {
		table.structure.Clear()
	}
	mts.currentIndex = 0
	mts.lastIndex = 0
	mts.maxTables = 0
	mts.tables = nil
}

// Delete record from structure. Returns error if key does not exist.
func (mts *Memtables) Delete(key []byte) error {
	return mts.tables[mts.currentIndex].structure.Delete(key)
}

// Get key from structure. Return error if key does not exist.
func (mts *Memtables) Get(key []byte) (*model.Record, error) {
	index := mts.currentIndex
	for {
		record, err := mts.tables[index].structure.Get(key)
		if err == nil {
			return record, nil
		}
		index = (index - 1) % mts.maxTables
		if index == mts.currentIndex {
			break
		}
	}
	return nil, errors.New("error: key '" + string(key) + "' not found in " + util.GetConfig().Memtable.Structure)
}

// IsFull returns true if all memtables are completely filled.
func (mts *Memtables) IsFull() bool {
	return mts.tables[mts.currentIndex].structure.IsFull() && (mts.currentIndex+1)%mts.maxTables == mts.lastIndex
}

// Flush returns all records from the last memtable, clears the memtable and rotates accordingly.
func (mts *Memtables) Flush() []model.Record {
	records := mts.tables[mts.lastIndex].structure.Flush()
	mts.tables[mts.lastIndex].structure.Clear()
	mts.lastIndex = (mts.lastIndex + 1) % mts.maxTables
	return records
}

func (mts *Memtables) getIterators() []iterator.Iterator {
	iterators := make([]iterator.Iterator, 0)
	for i := 0; i < mts.maxTables; i++ {
		mt := mts.tables[i]

		if iter, err := mt.structure.NewIterator(); err == nil {
			iterators = append(iterators, iter)
		}
	}
	return iterators
}

func (mts *Memtables) RangeScan(minValue []byte, maxValue []byte) []*model.Record {
	iterators := mts.getIterators()
	records := make([]*model.Record, 0)

	// Set all iterators to minValue (or first value greater than minValue)
	for i := 0; i < len(iterators); i++ {
		current := iterators[i]
		current.Next()

		for bytes.Compare(current.Value().Key, minValue) < 0 {
			if !current.Next() {
				break
			}
		}
	}

	seenValues := make(map[string]bool)
	for {
		minIndex := -1
		minKey := []byte{255}
		minTimestamp := uint64(math.MaxUint64)

		for i, iter := range iterators {
			if iter != nil && iter.Value() != nil && (bytes.Compare(iter.Value().Key, minKey) < 0 ||
				(bytes.Equal(iter.Value().Key, minKey) && iter.Value().Timestamp < minTimestamp)) {
				minIndex = i
				minKey = iter.Value().Key
				minTimestamp = iter.Value().Timestamp
			}
		}
		if minIndex == -1 {
			break
		}
		if !seenValues[string(minKey)] {
			if bytes.Compare(iterators[minIndex].Value().Key, maxValue) > 0 {
				iterators[minIndex] = nil
				continue
			}
			records = append(records, iterators[minIndex].Value())
			seenValues[string(minKey)] = true
		}

		if !iterators[minIndex].Next() || bytes.Compare(iterators[minIndex].Value().Key, maxValue) > 0 {
			iterators[minIndex] = nil
		}
	}
	return records
}
