package sstable

import (
	"fmt"
	"nasp-project/util"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

type SSTable struct {
	Data             DataBlock
	Index            IndexBlock
	Summary          SummaryBlock
	Filter           FilterBlock
	TOCFilename      string
	MetadataFilename string
}

// CreateSSTable creates an SSTable from the given data records and writes it to disk.
func CreateSSTable(recs []DataRecord, config util.SSTableConfig) (*SSTable, error) {
	label, err := getNextSStableLabel(config.SavePath)
	if err != nil {
		return nil, err
	}

	var sstable *SSTable = nil

	if config.SingleFile {
		// Starting offset for each block is calculated after the previous block is written.
		sstable = &SSTable{
			Data: DataBlock{
				Filename:    filepath.Join(config.SavePath, "usertable-"+label+"-SSTable.db"),
				StartOffset: 0,
			},
			Index: IndexBlock{
				Filename: filepath.Join(config.SavePath, "usertable-"+label+"-SSTable.db"),
			},
			Summary: SummaryBlock{
				Filename: filepath.Join(config.SavePath, "usertable-"+label+"-SSTable.db"),
			},
			Filter: FilterBlock{
				Filename: filepath.Join(config.SavePath, "usertable-"+label+"-SSTable.db"),
			},
			TOCFilename:      filepath.Join(config.SavePath, "usertable-"+label+"-L001-TOC.txt"),
			MetadataFilename: filepath.Join(config.SavePath, "usertable-"+label+"-Metadata.txt"),
		}
	} else {
		sstable = &SSTable{
			Data: DataBlock{
				Filename:    filepath.Join(config.SavePath, "usertable-"+label+"-Data.db"),
				StartOffset: 0,
			},
			Index: IndexBlock{
				Filename:    filepath.Join(config.SavePath, "usertable-"+label+"-Index.db"),
				StartOffset: 0,
			},
			Summary: SummaryBlock{
				Filename:    filepath.Join(config.SavePath, "usertable-"+label+"-Summary.db"),
				StartOffset: 0,
			},
			Filter: FilterBlock{
				Filename:    filepath.Join(config.SavePath, "usertable-"+label+"-Filter.db"),
				StartOffset: 0,
			},
			TOCFilename:      filepath.Join(config.SavePath, "usertable-"+label+"-L001-TOC.txt"),
			MetadataFilename: filepath.Join(config.SavePath, "usertable-"+label+"-Metadata.txt"),
		}
	}

	err = sstable.createFiles(config.SingleFile)
	if err != nil {
		e := sstable.deleteFiles()
		if e != nil {
			return nil, e
		}
		return nil, err
	}

	err = sstable.Data.Write(recs)
	if err != nil {
		return nil, err
	}

	if config.SingleFile {
		sstable.Index.StartOffset = sstable.Data.StartOffset + sstable.Data.Size
	}
	idxRecs, err := sstable.Index.CreateFromDataRecords(config.IndexDegree, recs)
	if err != nil {
		return nil, err
	}

	if config.SingleFile {
		sstable.Summary.StartOffset = sstable.Index.StartOffset + sstable.Index.Size
	}
	err = sstable.Summary.CreateFromIndexRecords(config.SummaryDegree, idxRecs)
	if err != nil {
		return nil, err
	}

	if config.SingleFile {
		sstable.Filter.StartOffset = sstable.Summary.StartOffset + sstable.Summary.Size
	}
	keys := make([][]byte, len(recs))
	for i, rec := range recs {
		keys[i] = rec.Key
	}
	sstable.Filter.CreateFilter(keys, config.FilterPrecision)
	err = sstable.Filter.Write()
	if err != nil {
		return nil, err
	}
	sstable.Filter.Filter = nil

	// TODO: Write Merkle Tree to Metadata file

	err = sstable.writeTOCFile()

	return sstable, nil
}

// getNextSStableLabel finds the largest label number in the given path and returns the next label number.
func getNextSStableLabel(path string) (string, error) {
	folder, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}

	re := regexp.MustCompile(`usertable-(\d+)-L(\d+)-TOC.txt`)

	maxNum := 0
	for _, file := range folder {
		if file.IsDir() {
			continue
		}
		match := re.FindStringSubmatch(file.Name())
		if match != nil {
			label, err := strconv.Atoi(match[1])
			if err != nil {
				return "", err
			}
			if label > maxNum {
				maxNum = label
			}
		}
	}

	return fmt.Sprintf("%05d", maxNum+1), nil
}

// createFiles creates empty files for the SSTable on disk.
func (sst *SSTable) createFiles(singleFile bool) error {
	file, err := os.Create(sst.TOCFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	file, err = os.Create(sst.MetadataFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	file, err = os.Create(sst.Data.Filename)
	if err != nil {
		return err
	}
	defer file.Close()

	if !singleFile {
		file, err = os.Create(sst.Index.Filename)
		if err != nil {
			return err
		}
		defer file.Close()

		file, err = os.Create(sst.Summary.Filename)
		if err != nil {
			return err
		}
		defer file.Close()

		file, err = os.Create(sst.Filter.Filename)
		if err != nil {
			return err
		}
		defer file.Close()
	}

	return nil
}

// deleteFiles deletes the files for the SSTable from disk.
func (sst *SSTable) deleteFiles() error {
	err := os.Remove(sst.TOCFilename)
	if err != nil {
		return err
	}

	err = os.Remove(sst.MetadataFilename)
	if err != nil {
		return err
	}

	err = os.Remove(sst.Data.Filename)
	if err != nil {
		return err
	}

	err = os.Remove(sst.Index.Filename)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	err = os.Remove(sst.Summary.Filename)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	err = os.Remove(sst.Filter.Filename)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

// writeTOCFile writes the TOC file to disk.
func (sst *SSTable) writeTOCFile() error {
	file, err := os.OpenFile(sst.TOCFilename, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	startOffset := strconv.FormatInt(sst.Data.StartOffset, 10)
	size := strconv.FormatInt(sst.Data.Size, 10)
	_, err = file.WriteString(startOffset + " " + size + " " + sst.Data.Filename + "\n")
	if err != nil {
		return err
	}

	startOffset = strconv.FormatInt(sst.Index.StartOffset, 10)
	size = strconv.FormatInt(sst.Index.Size, 10)
	_, err = file.WriteString(startOffset + " " + size + " " + sst.Index.Filename + "\n")
	if err != nil {
		return err
	}

	startOffset = strconv.FormatInt(sst.Summary.StartOffset, 10)
	size = strconv.FormatInt(sst.Summary.Size, 10)
	_, err = file.WriteString(startOffset + " " + size + " " + sst.Summary.Filename + "\n")
	if err != nil {
		return err
	}

	startOffset = strconv.FormatInt(sst.Filter.StartOffset, 10)
	size = strconv.FormatInt(sst.Filter.Size, 10)
	_, err = file.WriteString(startOffset + " " + size + " " + sst.Filter.Filename + "\n")
	if err != nil {
		return err
	}

	_, err = file.WriteString(sst.MetadataFilename + "\n")
	if err != nil {
		return err
	}

	return nil
}
