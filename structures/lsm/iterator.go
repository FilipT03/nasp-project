package lsm

import (
	"fmt"
	"nasp-project/structures/compression"
	"nasp-project/structures/sstable"
	"nasp-project/util"
	"os"
	"path/filepath"
)

func GetRangeIterators(startKey, endKey []byte, compressionDict *compression.Dictionary, config *util.Config) ([]util.Iterator, error) {
	var iterators []util.Iterator
	for lvl := 1; lvl <= config.LSMTree.MaxLevel; lvl++ {
		tables, err := GetSSTablesForLevel(lvl, config)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			it, err := table.NewRangeIterator(startKey, endKey, compressionDict)
			if err != nil {
				return nil, err
			}
			iterators = append(iterators, it)
		}
	}
	return iterators, nil
}

func GetPrefixIterators(prefix []byte, compressionDict *compression.Dictionary, config *util.Config) ([]util.Iterator, error) {
	var iterators []util.Iterator
	for lvl := 1; lvl <= config.LSMTree.MaxLevel; lvl++ {
		tables, err := GetSSTablesForLevel(lvl, config)
		if err != nil {
			return nil, err
		}
		for _, table := range tables {
			it, err := table.NewPrefixIterator(prefix, compressionDict)
			if err != nil {
				return nil, err
			}
			iterators = append(iterators, it)
		}
	}
	return iterators, nil
}

// TODO: Use function from lsm.go
func GetSSTablesForLevel(level int, config *util.Config) ([]*sstable.SSTable, error) {
	lvlLabel := fmt.Sprintf("L%03d", level)
	path := filepath.Join(config.SSTable.SavePath, lvlLabel, "TOC")
	folder, err := os.ReadDir(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if os.IsNotExist(err) {
		return nil, nil
	}

	var tables []*sstable.SSTable
	for _, file := range folder {
		if file.IsDir() {
			continue
		}
		table, err := sstable.OpenSSTableFromToc(filepath.Join(path, file.Name()))
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}
