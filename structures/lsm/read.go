package lsm

import (
	"bytes"
	"nasp-project/model"
	"nasp-project/structures/compression"
	"nasp-project/structures/sstable"
	"nasp-project/util"
)

// Read searches the LSM tree for the record with the given key.
// Returns the record if it is found, nil otherwise.
// The returned record is from the lowest LSM Tree level that contains the record.
// If the record is found in multiple same-level SSTables, the record with the latest timestamp is returned.
// Returns an error if the read fails.
func Read(key []byte, compressionDict *compression.Dictionary, config *util.Config) (*model.Record, error) {
	for lvl := 1; lvl <= config.LSMTree.MaxLevel; lvl++ {
		tables, err := GetSSTablesForLevel(config.SSTable.SavePath, lvl)
		if err != nil {
			return nil, err
		}

		if lvl > 1 && config.LSMTree.CompactionAlgorithm == "Leveled" {
			table, err := leveledFindTableWithKey(key, tables, compressionDict)
			if err != nil {
				return nil, err
			}
			if table != nil {
				tables = []*sstable.SSTable{table}
			} else {
				tables = nil
			}
		}

		var record *model.Record = nil
		for _, table := range tables {
			rec, err := table.Read(key, compressionDict)
			if err != nil {
				return nil, err
			}
			if rec == nil {
				continue
			}
			if record == nil || rec.Timestamp > record.Timestamp {
				record = rec
			}
		}

		if record != nil {
			return record, nil
		}
	}
	return nil, nil
}

// leveledFindTableWithKey returns the last from the list of tables that has a minimum key >= key.
// If the tables are a sorted, leveled compacted, LSMTree level, the result is the only table that may contain
// a record with the given key.
func leveledFindTableWithKey(key []byte, tables []*sstable.SSTable, compressionDict *compression.Dictionary) (*sstable.SSTable, error) {
	l, r := 0, len(tables)-1
	lp := -1
	for l <= r {
		m := l + (r-l)/2
		if !tables[m].Summary.HasRangeLoaded() {
			err := tables[m].Summary.LoadRange(compressionDict)
			if err != nil {
				return nil, err
			}
		}

		if bytes.Compare(tables[m].Summary.StartKey, key) >= 0 {
			lp = m
			l = m + 1
		} else {
			r = m - 1
		}

		tables[m].Summary.StartKey = nil
		tables[m].Summary.EndKey = nil
	}

	if lp == -1 {
		return nil, nil
	}
	return tables[lp], nil
}
