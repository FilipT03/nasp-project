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
func (gen *DataRecordGenerator) Merge(other *DataRecordGenerator, consumer DataRecordConsumer, skipDeleted ...bool) (err error) {
	defer func() {
		if cerr := gen.Clear(); cerr != nil {
			err = fmt.Errorf("%w && failed to clear this generator : %w", err, cerr)
		}

		if cerr := other.Clear(); cerr != nil {
			err = fmt.Errorf("%w && failed to clear other generator : %w", err, cerr)
		}
	}()

	return mergeGenerators(gen, other, consumer.Accept, skipDeleted...)
}

// TODO: should be double checked

// Preforms merging operation between the two given generators which must yield records with strictly increasing key values.
// It selects records in a way that corresponds to iterating through the result of merge operation on tables represented by the
// given generators and yields them to the consumer. The whole process is aborted if the consumer returns an error.
// If skipDeleted a record that is deleted, if it is relevant for the given key, will be skipped. By default no records are skipped.
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

	// points to the current record from the corresponding generator
	var rec1, rec2 *DataRecord
	// getting initial records to start the iteration
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

// ... merges generators writing the result into tables on level levelNum switching to a new table when data block limit is reached ...
func mergeGeneratorsWithLimit(
	gen1, gen2 *DataRecordGenerator,
	levelNum int,
	compressionDict *compression.Dictionary,
	sstableConfig *util.SSTableConfig,
	lsmConfig *util.LSMTreeConfig,
	skipDeleted ...bool,
) ([]*SSTable, error) {

	var tables []*SSTable
	var currentTable *SSTable
	var currentFile *os.File

	var currentWrittenBytes int64
	var currentWrittenRecords uint

	// switch to new SSTable, its data block and open its data block file
	newTable := func() error {
		nextTable, err := initializeSSTable(levelNum, sstableConfig)
		if err != nil {
			return fmt.Errorf("failed to switch to a new SSTable, couldn't initialize a new table at level %d : %w", levelNum, err)
		}
		// every time except the first time
		if currentTable != nil {
			// we add it even if build fails because it is already written to disc
			tables = append(tables, currentTable)

			if berr := currentTable.BuildFromDataBlock(currentWrittenRecords, compressionDict, sstableConfig); berr != nil {
				return fmt.Errorf("failed to build table at level %d from data block '%s' : %w", levelNum, currentTable.Data.Filename, berr)
			}
		}

		currentTable = nextTable
		currentWrittenRecords = 0
		currentWrittenBytes = 0

		if currentFile != nil {
			currentFile.Close() // should this err be checked?
		}

		currentFile, err = os.OpenFile(nextTable.Data.Filename, os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to switch to a new SSTable, couldn't open the data block file '%s' : %w", nextTable.Data.Filename, err)
		}
		// see if should seek begining of db
		return nil
	}

	// adding initial table to the tables
	if err := newTable(); err != nil {
		return nil, err
	}

	// write to current datablock and switch tables if limit is reached
	writer := func(record *DataRecord) error {
		numBytes, err := currentTable.Data.writeRecordLen(currentFile, record, compressionDict)
		if err != nil {
			return fmt.Errorf("failed to write record : %w", err)
		}

		currentWrittenRecords++
		currentWrittenBytes += int64(numBytes)

		if currentWrittenBytes > lsmConfig.Leveled.DataBlockSize {
			if err := newTable(); err != nil {
				return err
			}
		}

		return nil
	}

	err := mergeGenerators(gen1, gen2, writer, skipDeleted...)
	return tables, err
}

// Merges the table with run writing the result SSTables with size limited by the config to the level with the given levelNum.
// The newly generated tables are labeled with the first free label in the level, and their proxy objects are returned as a slice.
// It skips deleted records if possible (when merging from a run into the last level run).
// Returns error if merging or any part of the cleanup fails.
// If merging is successfull the input table files are deleted, otherwise the lsm tree will stay unchanged.
func MergeTableWithRun(
	compressionDict *compression.Dictionary,
	sstableConfig *util.SSTableConfig,
	lsmConfig *util.LSMTreeConfig,
	levelNum int,
	table *SSTable,
	run ...*SSTable,
) (newTables []*SSTable, err error) { // could make this take two runs
	// used for merging
	var tableGen, runGen *DataRecordGenerator

	tableGen, err = NewDataRecordGenerator([]*DataBlock{&table.Data}, compressionDict)
	if err != nil {
		return
	}

	// sstable -> data block
	runBlocks := make([]*DataBlock, len(run))
	for _, table := range run {
		runBlocks = append(runBlocks, &table.Data) // just in case
	}

	runGen, err = NewDataRecordGenerator(runBlocks, compressionDict)
	if err != nil {
		return
	}

	// cleanup after we are done
	defer func() {
		if err == nil {
			// merge was successfull deleting files for the old SSTables from disc
			for _, table := range append(run, table) {
				if cerr := table.deleteFiles(); cerr != nil {
					err = fmt.Errorf("%w && failed to delete files for SSTable '%s' from disc : %w", err, table.TOCFilename, cerr)
				}
				// we delete all tables even if one deletion fails
			}
		} else {
			// merge was unsuccessfull deleting new tables that were created in the partial merge
			var derr error
			for _, newTable := range newTables {
				if err := newTable.deleteFiles(); err != nil {
					derr = fmt.Errorf("%w && failed to delete files for newly created SSTable '%s' ", derr, newTable.TOCFilename)
				}
				// we delete all tables even if one deletion fails
			}

			if derr == nil {
				err = fmt.Errorf("failed to merge the given tables : %w", err)
			} else {
				err = fmt.Errorf("failed to merge the given tables : %w [while handling the previous error new one occured : %w]", err, derr)
			}
		}

		// clearing generators
		for _, gen := range [...]*DataRecordGenerator{tableGen, runGen} {
			if cerr := gen.Clear(); cerr != nil {
				err = fmt.Errorf("%w && failed to clear %v : %w", err, gen, cerr)
			}
			// we clear all generators even if one clearance fails
		}
	}()

	maxLevelNum := lsmConfig.MaxLevel - 1 + util.LSMFirstLevelNum
	// skip deleted only if we are merging into the last level and the penultimate level is a run (big sstable partitioned into multiple smaller ones)
	skipDeleted := lsmConfig.MaxLevel > 2 && levelNum == maxLevelNum

	newTables, err = mergeGeneratorsWithLimit(tableGen, runGen, levelNum, compressionDict, sstableConfig, lsmConfig, skipDeleted)
	return newTables, err
}
