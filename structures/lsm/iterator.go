package lsm

import (
	"nasp-project/structures/compression"
	"nasp-project/util"
)

func GetRangeIterators(startKey, endKey []byte, compressionDict *compression.Dictionary, config *util.Config) ([]util.Iterator, error) {
	var iterators []util.Iterator
	for lvl := 1; lvl <= config.LSMTree.MaxLevel; lvl++ {
		tables, err := GetSSTablesForLevel(config.SSTable.SavePath, lvl)
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
		tables, err := GetSSTablesForLevel(config.SSTable.SavePath, lvl)
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
