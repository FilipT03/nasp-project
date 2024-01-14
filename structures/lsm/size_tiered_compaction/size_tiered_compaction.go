package size_tiered_compaction

import (
	"fmt"
	"io/ioutil"
	"nasp-project/structures/sstable"
	"nasp-project/util"
)

// Compact performs compaction on the LSM tree.
func Compact(sstableConfig *util.SSTableConfig, lsmConfig *util.LSMTreeConfig) {
	// maximum number of levels in the LSM tree
	maxLsmLevel := lsmConfig.MaxLevel
	// maximum number of SSTables in each level of the LSM Tree
	maxLsmNodesPerLevel := lsmConfig.MaxLsmNodesPerLevel
	// filepath
	filepath := sstableConfig.SavePath

	level := 1
	// while the current level is less than the maximum level
	// we don't compact the last level as it is the level where the data is stored
	for level < maxLsmLevel {
		// path to the TOC file of the current level
		pathToToc := filepath + "/L" + fmt.Sprintf("%03d", level) + "/TOC"
		// search for all SSTables in the current level
		fileNames := FindSSTables(pathToToc)
		// if there are no SSTables in the current level
		if len(fileNames) == 0 {
			return
		}
		// if there is only one SSTable in the current level
		if level == 1 {
			if len(fileNames) == 1 {
				return
			}
			if len(fileNames) < maxLsmNodesPerLevel {
				return
			}
		}
		// if the number of SSTables is greater than the maximum number of nodes in the current level
		// then compact the SSTables
		// do the same for the next level
		// until the last level is reached
		// now I'm compressing
		// while the number of SSTables is greater than the maximum number of nodes in the current level
		for len(fileNames) >= maxLsmNodesPerLevel {
			// if len(fileNames) == 1 then there is only one SSTable in the current level
			// and no compaction is needed
			if len(fileNames) == 1 {
				break
			}
			// merge the first two SSTables from fileNames
			sstable1, err := sstable.OpenSSTableFromToc(pathToToc + "/" + fileNames[0])
			if err != nil {
				return
			}
			sstable2, err := sstable.OpenSSTableFromToc(pathToToc + "/" + fileNames[1])
			if err != nil {
				return
			}
			// merge the two SSTables and save the result in the next level
			_, err = sstable.MergeSSTables(sstable1, sstable2, level+1, sstableConfig)
			if err != nil {
				return
			}
			// set fileNames to the remaining SSTables
			fileNames = FindSSTables(pathToToc)
		}

		level++
	}
}

// FindSSTables returns the names of the SSTables in the given directory.
func FindSSTables(filepath string) []string {
	// read the directory
	files, _ := ioutil.ReadDir(filepath)
	// used for storing the names of the SSTables
	var sstableNames []string
	// for each filename in directory I want to append it to the sstableNames
	for _, file := range files {
		sstableNames = append(sstableNames, file.Name())
	}

	return sstableNames
}
