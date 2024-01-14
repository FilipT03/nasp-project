package lsm

import (
	"fmt"
	"nasp-project/model"
	"nasp-project/structures/lsm/size_tiered_compaction"
	"nasp-project/structures/sstable"
	"nasp-project/util"
	"os"
	"path/filepath"
)

// Read searches the LSM tree for the record with the given key.
// Returns the record if it is found, nil otherwise.
// The returned record is from the lowest LSM Tree level that contains the record.
// If the record is found in multiple same-level SSTables, the record with the latest timestamp is returned.
// Returns an error if the read fails.
func Read(key []byte, config *util.Config) (*model.Record, error) {
	for lvl := 1; lvl <= config.LSMTree.MaxLevel; lvl++ {
		lvlLabel := fmt.Sprintf("L%03d", lvl)
		path := filepath.Join(config.SSTable.SavePath, lvlLabel, "TOC")
		folder, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}

		var record *model.Record = nil
		for _, file := range folder {
			if file.IsDir() {
				continue
			}
			table, err := sstable.OpenSSTableFromToc(filepath.Join(path, file.Name()))
			if err != nil {
				return nil, err
			}
			rec, err := table.Read(key)
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

// Compact compacts the LSM tree by merging SSTables from the same level.
// Runs compaction only if the compaction start condition is met.
// The compaction algorithm used is determined by the config.
func Compact(config *util.LSMTreeConfig, sstConfig *util.SSTableConfig) error {
	if config.CompactionAlgorithm == "Size-Tiered" {
		// TODO: Add condition for compaction call
		size_tiered_compaction.Compact(sstConfig, config)
	} else if config.CompactionAlgorithm == "Leveled" {
		// TODO: Add condition for compaction call
		// TODO: Implement
	}
	return nil
}
