package leveled_compaction

import (
	"fmt"
	"nasp-project/structures/compression"
	"nasp-project/structures/sstable"
)

// TODO: implement
func ShouldCompact(levelNum, maxLevelNum, firstLevelMaxSizeBytes int, level ...*sstable.SSTable) (bool, error) {
	return false, fmt.Errorf("not implemented yet :(")
}

// TODO: fix prototype so it is adequate and implement
func Compact(compressionDict *compression.Dictionary, config ...string) {
}
