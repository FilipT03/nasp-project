package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"nasp-project/structures/compression"
	"nasp-project/util"
	"os"
)

// MergeSSTables merges the given SSTables and writes the result to disk.
// Removes the input SSTables from disk.
// Returns the new SSTable.
// Returns an error if the merge fails.
func MergeSSTables(sst1, sst2 *SSTable, level int, config *util.SSTableConfig, compressionDict *compression.Dictionary) (*SSTable, error) {
	sstable, err := initializeSSTable(level, config)
	if err != nil {
		return nil, err
	}

	numRecords, err := sstable.Data.WriteMerged(&sst1.Data, &sst2.Data, compressionDict)
	if err != nil {
		return nil, err
	}

	err = sstable.BuildFromDataBlock(numRecords, compressionDict, config)
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

// MergeMultipleSSTables merges the given SSTables and writes the result to disk.
// Removes the input SSTables from disk.
// Returns the newly created SSTable.
// Returns an error if the merge fails.
func MergeMultipleSSTables(tables []*SSTable, level int, config *util.SSTableConfig, compressionDict *compression.Dictionary) (*SSTable, error) {
	if len(tables) < 1 {
		return nil, errors.New("no tables to merge")
	}
	var numRecs uint
	for len(tables) > 1 {
		var newTables []*SSTable
		for i := 0; i+1 < len(tables); i += 2 {
			newTable, err := initializeSSTable(level, config)
			if err != nil {
				return nil, err
			}
			numRecs, err = newTable.Data.WriteMerged(&tables[i].Data, &tables[i+1].Data, compressionDict)
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
		if len(tables)%2 != 0 {
			newTables = append(newTables, tables[len(tables)-1])
		}
		tables = newTables
	}
	err := tables[0].BuildFromDataBlock(numRecs, compressionDict, config)
	if err != nil {
		return nil, err
	}
	return tables[0], nil
}

// chose which record should be written to the SSTable
func chooseRecord(a, b *DataRecord) *DataRecord {
	cmp := bytes.Compare(a.Key, b.Key)
	switch cmp {
	case -1:
		{
			// a has smaller key, pick a
			return a
		}
	case 1:
		{
			// b has smaller key, pick b
			return b
		}
	}
	// a and b have same keys
	if a.Timestamp > b.Timestamp {
		// a is newer, pick a
		return a
	}
	// b is newer or both have same timestamp, return b
	return b
}

// DataRecordConsumer is an interface used for consuming DataRecord instances.
type DataRecordConsumer interface {
	// Accept is a method that processes a DataRecord
	// It returns an error if the consumption fails.
	Accept(*DataRecord) error
}

// Interesting idea to make a method for merging generators?
func (gen *DataRecordGenerator) Merge(other *DataRecordGenerator, consumer DataRecordConsumer, skipDeleted ...bool) error {
	// TODO: maby should clear the generators afterwards
	defer func() {
		other.Clear()
		gen.Clear()
	}()

	return mergeGenerators(gen, other, consumer.Accept, skipDeleted...)
}

// TODO: should be double checked
// ...
func mergeGenerators(gen1, gen2 *DataRecordGenerator, consumer func(*DataRecord) error, skipDeleted ...bool) (err error) {

	getNextRecord := func(generator *DataRecordGenerator, record **DataRecord) {
		if *record, err = generator.GetNextRecord(); err != nil {
			err = fmt.Errorf("failed to merge, %v returned error : %w", generator, err)
		}
	}

	skip := len(skipDeleted) != 0 && skipDeleted[0] // default don't skip
	sendRecord := func(record *DataRecord) {
		if !skip || !record.Tombstone {
			if err = consumer(record); err != nil {
				err = fmt.Errorf("failed to merge, consumption failed : %w", err)
			}
		}
	}

	// getting initial records to start iteration
	var rec1, rec2 *DataRecord

	if getNextRecord(gen1, &rec1); err != nil {
		return err
	}

	if getNextRecord(gen2, &rec2); err != nil {
		return err
	}

	var sentinel *DataRecord = nil
	// loop until both generators are exausted
	for rec1 != sentinel || rec2 != sentinel {

		if rec1 == sentinel {
			// first generator is exausted, sending second
			if sendRecord(rec2); err != nil {
				return err
			}

			if getNextRecord(gen2, &rec2); err != nil {
				return err
			}

			continue
		}

		if rec2 == sentinel {
			// second generator is exausted, sending first
			if sendRecord(rec1); err != nil {
				return err
			}

			if getNextRecord(gen1, &rec1); err != nil {
				return err
			}

			continue
		}

		// neither generator is exausted, comparing records and selecting one to send
		theChosenOne := chooseRecord(rec1, rec2)
		if sendRecord(theChosenOne); err != nil {
			return err
		}

		// getting next record for the sent one or for both if their keys were the same (if keys are same we skip the irrelevant one)
		keysAreEqual := bytes.Equal(rec1.Key, rec2.Key)

		if rec1 == theChosenOne || keysAreEqual {
			if getNextRecord(gen1, &rec1); err != nil {
				return err
			}
		}

		if rec2 == theChosenOne || keysAreEqual {
			if getNextRecord(gen2, &rec2); err != nil {
				return err
			}
		}
	}

	return nil
}

const lsmFirstLevel int = 1

// ...
func mergeGeneratorsWithLimit(
	gen1, gen2 *DataRecordGenerator,
	levelNum int,
	compressionDict *compression.Dictionary,
	sstableConfig *util.SSTableConfig,
	lsmConfig *util.LSMTreeConfig,
) ([]*SSTable, error) {

	var (
		tables []*SSTable

		currentDataBlock *DataBlock
		currentFile      *os.File

		currentWritten int64 = 0
	)

	newTable := func() error {
		table, err := initializeSSTable(levelNum, sstableConfig)
		if err != nil {
			return err
		}

		tables = append(tables, table)
		currentDataBlock = &table.Data

		if currentFile != nil {
			currentFile.Close()
		}

		currentFile, err = os.OpenFile(table.Data.Filename, os.O_WRONLY, 0644)
		if err != nil {
			return err
		}

		return nil
	}

	if err := newTable(); err != nil {
		return nil, err
	}

	writer := func(record *DataRecord) error {
		numBytes, err := currentDataBlock.writeRecordLen(currentFile, record, compressionDict)
		if err != nil {
			return err
		}

		currentWritten += int64(numBytes)
		if currentWritten > lsmConfig.Leveled.DataBlockSize {
			if err := newTable(); err != nil {
				return err
			}

			currentWritten = 0
		}

		return nil
	}

	err := mergeGenerators(gen1, gen2, writer, levelNum == lsmConfig.MaxLevel-lsmFirstLevel)
	if err != nil {
		return nil, err
	}

	return tables, nil
}

// ...
func MergeTableWithRun(table *SSTable, run ...*SSTable) {

}
