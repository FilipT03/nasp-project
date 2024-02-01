package leveled_compaction

import (
	"fmt"
	"nasp-project/structures/compression"
	"nasp-project/structures/lsm"
	"nasp-project/structures/sstable"
	"nasp-project/util"
)

const useSpecialSelectionForFirstLevel = true

// Check if the level needs compaction according to the given lsmConfig. The level given must be a valid level.
// If SSTables are not provided it fetches them and then checks, otherwise assumes that the given tables are all tables from the level.
// In case the tables are provided sstableConfig may be nil.
func ShouldCompact(
	levelNum int,
	sstableConfig *util.SSTableConfig,
	lsmConfig *util.LSMTreeConfig,
	level ...*sstable.SSTable,
) (bool, error) {

	maxLevelNum := lsmConfig.MaxLevel - 1 + util.LSMFirstLevelNum // we will always have at most MaxLevel levels
	if levelNum == maxLevelNum {
		// the last level should never be compacted
		return false, nil
	}

	var tables []*sstable.SSTable

	if len(level) == 0 {
		// SSTables from the level are not provided, so we fetch them
		var err error
		if tables, err = lsm.GetSSTablesForLevel(sstableConfig.SavePath, levelNum); err != nil {
			return false, fmt.Errorf("failed to check if the level needs compaction : %w", err)
		}
	} else {
		// assuming that the provided sstables in level are all tables in the level
		tables = level
	}

	maxLevelSize := calcSize(int64(levelNum), lsmConfig.Leveled.FirstLevelTotalDataSize, int64(lsmConfig.Leveled.FanoutSize))
	return exceedsSize(tables, maxLevelSize), nil
}

// returns whether or not the given level total data block size is larger than the given size
func exceedsSize(level []*sstable.SSTable, size int64) bool {
	levelSize := int64(0)

	for _, table := range level {
		levelSize += table.Data.Size // TODO: see if this is the correct way to get the size of data block

		if levelSize > size {
			return true
		}
	}

	return false
}

// calculates the max size for a level
func calcSize(levelNum, firstLevelTotalDataSize, fanoutSize int64) int64 {
	result := firstLevelTotalDataSize
	for i := int64(0); i < levelNum-util.LSMFirstLevelNum; i++ {
		result *= fanoutSize
	}

	return result
}

// ...
func Compact(compressionDict *compression.Dictionary, sstableConfig *util.SSTableConfig, lsmConfig *util.LSMTreeConfig) error {
	return triggerCompaction(util.LSMFirstLevelNum, compressionDict, sstableConfig, lsmConfig)
}

func triggerCompaction(
	levelNum int,
	compressionDict *compression.Dictionary,
	sstableConfig *util.SSTableConfig,
	lsmConfig *util.LSMTreeConfig,
) error {

	hadCompacted := false
	// we do compactions from this level untill it reaches the desired size and only then call the compaction from next level
	for ; ; hadCompacted = true {
		shouldCompact, err := ShouldCompact(levelNum, sstableConfig, lsmConfig)
		if err != nil {
			return fmt.Errorf("compaction from level %d failed : %w", levelNum, err)
		}

		if !shouldCompact {
			break
		}

		// table from this level that will take part in the compaction
		var selectedTable *sstable.SSTable
		// selected tables and result tables from the next level
		var overlapTables, resultTables []*sstable.SSTable

		// selecting the first table
		if useSpecialSelectionForFirstLevel && levelNum == util.LSMFirstLevelNum {
			// special selection only at first level where memtables are flushed to
			selectedTable, err = selectTableFirstLevel(compressionDict, sstableConfig) // modifies the first level
		} else {
			selectedTable, err = selectTable(sstableConfig.SavePath, levelNum)
		}

		if err != nil {
			return fmt.Errorf("compaction from level %d failed, couldn't select the table for compaction : %w", levelNum, err)
		}

		// making sure that we can check the key range from the selected table
		if !selectedTable.Summary.HasRangeLoaded() {
			if err := selectedTable.Summary.LoadRange(compressionDict); err != nil {
				return fmt.Errorf("compaction from level %d failed, couldn't load summary for selected table : %w", levelNum, err)
			}
		}

		nextLevelNum := levelNum + 1
		// selecting the range of tables from next level to compact with
		overlapTables, err = getSSTablesForLevelThatOverlapRange(levelNum, selectedTable.Summary.StartKey, selectedTable.Summary.EndKey, sstableConfig.SavePath, compressionDict)
		if err != nil {
			// this may happen if summary check for next level tables fails
			return fmt.Errorf("compaction from level %d failed, couldn't select overlap tables from next level : %w", levelNum, err)
		}

		// merge the selectedTable and overlapTables writing resultTables to next level
		resultTables, err = sstable.MergeTableWithRun(compressionDict, sstableConfig, lsmConfig, nextLevelNum, selectedTable, overlapTables...)
		if err != nil {
			return fmt.Errorf("compaction from level %d failed, couldn't merge the selected tables into next level : %w", levelNum, err)
		}

		// TODO: need to update the SSTable labels from next level now because they are all messed up
		fmt.Printf("resultTables: %v\n", resultTables)
	}

	// we trigger compaction from next level only if a compaction from this level occured
	if hadCompacted {
		return triggerCompaction(levelNum+1, compressionDict, sstableConfig, lsmConfig)
	}

	return nil
}

func selectTableFirstLevel(compressionDict *compression.Dictionary, sstableConfig *util.SSTableConfig) (*sstable.SSTable, error) {
	level, err := lsm.GetSSTablesForLevel(sstableConfig.SavePath, util.LSMFirstLevelNum)
	if err != nil {
		return nil, err
	}

	if len(level) == 0 {
		return nil, fmt.Errorf("failed to select table from level %d : level is empty", util.LSMFirstLevelNum)
	}

	// we merge all tables from first level into one, and that is the selected one
	selected, merr := sstable.MergeMultipleSSTables(level, util.LSMFirstLevelNum, sstableConfig, compressionDict)
	if merr != nil {
		return nil, merr
	}

	return selected, nil
}

func selectTable(savePath string, levelNum int) (*sstable.SSTable, error) {
	level, err := lsm.GetSSTablesForLevel(savePath, levelNum)
	if err != nil {
		return nil, err
	}

	if len(level) == 0 {
		return nil, fmt.Errorf("failed to select table from level %d : level is empty", levelNum)
	}

	return level[0], nil // maby temporary maby change :)
}

func getSSTablesForLevelThatOverlapRange(
	levelNum int,
	minKey, maxKey []byte,
	savePath string,
	compressionDict *compression.Dictionary,
) ([]*sstable.SSTable, error) {

	tables, err := lsm.GetSSTablesForLevel(savePath, levelNum)
	if err != nil {
		return nil, err
	}

	var selection []*sstable.SSTable

	for _, table := range tables {
		// ensuring that we can check the key range from summary of the table
		if !table.Summary.HasRangeLoaded() {
			err = table.Summary.LoadRange(compressionDict)
			if err != nil {
				return nil, err
			}
		}

		// TODO: add condition

		if 0 == 0 {
			selection = append(selection, table)
		}
	}

	return selection, nil
}

func fixLabelsForLevel(addedTables []*sstable.SSTable, levelNum int) error {
	return fmt.Errorf("not implemented yet :(")
}
