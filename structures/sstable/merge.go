package sstable

import (
	"errors"
	"nasp-project/util"
)

// MergeSSTables merges the given SSTables and writes the result to disk.
// Removes the input SSTables from disk.
// Returns the new SSTable.
// Returns an error if the merge fails.
func MergeSSTables(sst1, sst2 *SSTable, level int, config util.SSTableConfig) (*SSTable, error) {
	sstable, err := initializeSSTable(level, config)
	if err != nil {
		return nil, err
	}

	numRecords, err := sstable.Data.WriteMerged(&sst1.Data, &sst2.Data)
	if err != nil {
		return nil, err
	}

	err = sstable.BuildFromDataBlock(numRecords, &config)
	if err != nil {
		return nil, err
	}

	err = sst1.deleteFiles()
	if err != nil {
		return nil, err
	}

	err = sst2.deleteFiles()
	if err != nil {
		return nil, err
	}

	return sstable, nil
}

func MergeMultipleSSTables(tables []*SSTable, level int, config *util.SSTableConfig) (*SSTable, error) {
	if len(tables) < 1 {
		return nil, errors.New("no tables to merge")
	}
	var numRecs uint
	for len(tables) > 1 {
		var newTables []*SSTable
		for i := 0; i < len(tables); i += 2 {
			newTable, err := initializeSSTable(level, *config)
			if err != nil {
				return nil, err
			}
			numRecs, err = newTable.Data.WriteMerged(&tables[i].Data, &tables[i+1].Data)
			if err != nil {
				return nil, err
			}

			newTables = append(newTables, newTable)

			err = tables[i].deleteFiles()
			if err != nil {
				return nil, err
			}
			err = tables[i+1].deleteFiles()
			if err != nil {
				return nil, err
			}
		}
		tables = newTables
	}
	err := tables[0].BuildFromDataBlock(numRecs, config)
	if err != nil {
		return nil, err
	}
	return tables[0], nil
}
