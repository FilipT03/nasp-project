package leveled_compaction

import (
	"fmt"
	"nasp-project/structures/compression"
	"nasp-project/structures/lsm"
	"nasp-project/structures/sstable"
	"nasp-project/util"
)

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
			return false, fmt.Errorf("failed to check if level %d needs compaction : %w", levelNum, err)
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

// TODO: fix prototype so it is adequate and implement
func Compact(compressionDict *compression.Dictionary, sstableConfig *util.SSTableConfig, lsmConfig *util.LSMTreeConfig) {

}
