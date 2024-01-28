package compactions

import (
	"nasp-project/structures/compression"
	"nasp-project/structures/lsm/compactions/size_tiered_compaction"
	"nasp-project/util"
)

// Compact compacts the LSM tree by merging SSTables from the same level.
// Runs compaction only if the compaction start condition is met.
// The compaction algorithm used is determined by the config.
func Compact(compressionDict *compression.Dictionary, config *util.LSMTreeConfig, sstConfig *util.SSTableConfig) error {
	if config.CompactionAlgorithm == "Size-Tiered" {
		// TODO: Add condition for compaction call
		size_tiered_compaction.Compact(compressionDict, sstConfig, config)
	} else if config.CompactionAlgorithm == "Leveled" {
		// TODO: Add condition for compaction call
		// TODO: Implement
	}
	return nil
}
