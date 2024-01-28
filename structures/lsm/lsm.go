package lsm

import (
	"fmt"
	"nasp-project/model"
	"nasp-project/structures/compression"
	"nasp-project/structures/sstable"
	"nasp-project/util"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

/*
	=== SSTable save directory organisation ===

	- SSTable.savePath
  		- /^L\d{3,}$/
    		- /^TOC$/
      			- /^usertable-(\d{5,})-TOC.txt$/
      			- ...
    		- /^usertable-(\d{5,})-Metadata.txt$/
    		- /^usertable-(\d{5,})-SSTable.db$/
    		- ...
  		- ...


	If multiple files are used to store elements of a table then a single SSTable file is broken into:

		/^usertable-(\d{5,})-Data.db$/
		/^usertable-(\d{5,})-Index.db$/
  		/^usertable-(\d{5,})-Summary.db$/
  		/^usertable-(\d{5,})-Filter.db$/
*/

const (
	// The level number of the first level of SSTables in the LSM tree. This is where Memtables are flushed to.
	FirstLevelNum = 1
)

func levelDirPath(savePath string, level int) string {
	return filepath.Join(savePath, fmt.Sprintf("L%03d", level))
}

func tOCDirPath(savePath string, level int) string {
	return filepath.Join(levelDirPath(savePath, level), "TOC")
}

func tOCFileName(labelNum int) string {
	return fmt.Sprintf("usertable-%05d-TOC.txt", labelNum)
}

// Returns the file paths for TOC files from every SSTable on the given level.
// It takes savePath, the base directory where all levels of LSM tree are stored, and the level to consider.
// Returns error if reading the TOC directory of the level fails.
func GetTOCFilePathsForLevel(savePath string, level int) ([]string, error) {
	dir := tOCDirPath(savePath, level)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read level %d TOC directory '%s' : %w", level, dir, err)
	}

	tocPaths := []string{}
	for _, entry := range entries {
		if !entry.IsDir() {
			// we assume that all files in toc directory are toc files
			tocPaths = append(tocPaths, filepath.Join(dir, entry.Name()))
		}
	}

	return tocPaths, nil
}

// Get all SSTables from the given level. It takes savePath, a base directory where all levels of LSM tree are stored, and the level to consider.
// If reading a TOC directory fails it returns nil and error.
// If opening an SSTable from a TOC file fails it returns a slice of successfully opened SSTables and error.
func GetSSTablesForLevel(savePath string, level int) ([]*sstable.SSTable, error) {
	tocPaths, err := GetTOCFilePathsForLevel(savePath, level)
	if err != nil {
		return nil, err
	}

	var tables []*sstable.SSTable

	for _, tocPath := range tocPaths {
		table, err := sstable.OpenSSTableFromToc(tocPath)
		if err != nil {
			return tables, fmt.Errorf("failed to open SSTable from TOC file '%s' : %w", tocPath, err)
		}

		tables = append(tables, table)
	}

	return tables, nil
}

// extracts label number from TOC file name
func getLabelNumFromSSTable(table *sstable.SSTable) int {
	re := regexp.MustCompile(`usertable-(\d+)-TOC.txt`)
	match := re.FindStringSubmatch(table.TOCFilename)
	if match != nil {
		labelNum, err := strconv.Atoi(match[1])

		if err == nil {
			return labelNum
		}
	}

	return -1
}

// In place sort the given slice of SSTables based on their label numbers
// in ascending order by default or in descending order if reverse.
func SortSSTablesByLabelNum(tables []*sstable.SSTable, reverse ...bool) {
	inAscendingOrder := len(reverse) == 0 || reverse[0]

	if inAscendingOrder {
		// sorting in ascending order from smallest label number to largest
		sort.Slice(tables, func(i, j int) bool {
			return getLabelNumFromSSTable(tables[i]) < getLabelNumFromSSTable(tables[j])
		})
	} else {
		// sorting in descending order from largest label number to smallest
		sort.Slice(tables, func(i, j int) bool {
			return getLabelNumFromSSTable(tables[i]) > getLabelNumFromSSTable(tables[j])
		})
	}
}

// Climbs up the LSM tree reading level by level untill a reading fails, then returns a slice of successfully fetched levels.
// If sortedByLabel the SSTables within a level will be sorted in ascending order by their label number (no sorting is done by default).
func GetLSMTreeNoexcept(savePath string, sortedByLabel ...bool) (levels [][]*sstable.SSTable) {
	sort := len(sortedByLabel) != 0 && sortedByLabel[0]

	for levelNum := FirstLevelNum; ; levelNum++ {
		level, err := GetSSTablesForLevel(savePath, levelNum)
		if err != nil {
			break
		}

		levels = append(levels, level)

		if sort {
			SortSSTablesByLabelNum(level)
		}
	}

	return
}

// TODO: implement if above is not sufficient
func GetLSMTree( /*...*/ )/* (...)*/ {}

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
