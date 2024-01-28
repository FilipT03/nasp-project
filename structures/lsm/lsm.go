package lsm

import (
	"fmt"
	"nasp-project/model"
	"nasp-project/structures/compression"
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
func Read(key []byte, compressionDict *compression.Dictionary, config *util.Config) (*model.Record, error) {
	for lvl := 1; lvl <= config.LSMTree.MaxLevel; lvl++ {
		lvlLabel := fmt.Sprintf("L%03d", lvl)
		path := filepath.Join(config.SSTable.SavePath, lvlLabel, "TOC")
		folder, err := os.ReadDir(path)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if os.IsNotExist(err) {
			continue
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
