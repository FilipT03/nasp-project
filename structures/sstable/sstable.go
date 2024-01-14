package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"nasp-project/model"
	"nasp-project/structures/merkle_tree"
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

func initializeSSTable(level int, config *util.SSTableConfig) (*SSTable, error) {
	path := filepath.Join(config.SavePath, fmt.Sprintf("L%03d", level))

	labelNum, err := GetNextSStableLabel(filepath.Join(path, "TOC"))
	if err != nil {
		return nil, err
	}
	label := fmt.Sprintf("%05d", labelNum)

	var sstable *SSTable = nil

	if config.SingleFile {
		// Starting offset for each block is calculated after the previous block is written.
		sstable = &SSTable{
			Data: DataBlock{
				util.BinaryFile{
					Filename:    filepath.Join(path, "usertable-"+label+"-SSTable.db"),
					StartOffset: 0,
				},
			},
			Index: IndexBlock{
				util.BinaryFile{
					Filename: filepath.Join(path, "usertable-"+label+"-SSTable.db"),
				},
			},
			Summary: SummaryBlock{
				BinaryFile: util.BinaryFile{
					Filename: filepath.Join(path, "usertable-"+label+"-SSTable.db"),
				},
			},
			Filter: FilterBlock{
				BinaryFile: util.BinaryFile{
					Filename: filepath.Join(path, "usertable-"+label+"-SSTable.db"),
				},
			},
			TOCFilename:      filepath.Join(path, "TOC", "usertable-"+label+"-TOC.txt"),
			MetadataFilename: filepath.Join(path, "usertable-"+label+"-Metadata.txt"),
		}
	} else {
		sstable = &SSTable{
			Data: DataBlock{
				util.BinaryFile{
					Filename:    filepath.Join(path, "usertable-"+label+"-Data.db"),
					StartOffset: 0,
				},
			},
			Index: IndexBlock{
				util.BinaryFile{
					Filename:    filepath.Join(path, "usertable-"+label+"-Index.db"),
					StartOffset: 0,
				},
			},
			Summary: SummaryBlock{
				BinaryFile: util.BinaryFile{
					Filename:    filepath.Join(path, "usertable-"+label+"-Summary.db"),
					StartOffset: 0,
				},
			},
			Filter: FilterBlock{
				BinaryFile: util.BinaryFile{
					Filename:    filepath.Join(path, "usertable-"+label+"-Filter.db"),
					StartOffset: 0,
				},
			},
			TOCFilename:      filepath.Join(path, "TOC", "usertable-"+label+"-TOC.txt"),
			MetadataFilename: filepath.Join(path, "usertable-"+label+"-Metadata.txt"),
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

	return sstable, nil
}

// CreateSSTable creates an SSTable from the given data records and writes it to disk.
func CreateSSTable(records []model.Record, config *util.SSTableConfig) (*SSTable, error) {
	sstable, err := initializeSSTable(1, config) // when creating from memory, always save to the level no 1
	if err != nil {
		return nil, err
	}

	recs := dataRecordsFromRecords(records)

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

	files := sstable.toBinaryFiles()
	merkleTree := merkle_tree.NewMerkleTree(files, config.MerkleTreeChunkSize)
	file, err := os.Create(sstable.MetadataFilename)
	_, err = file.WriteString(merkleTree.Serialize())
	if err != nil {
		return nil, err
	}

	err = sstable.writeTOCFile()

	return sstable, nil
}

func (sst *SSTable) toBinaryFiles() []util.BinaryFile {
	return []util.BinaryFile{
		{
			Filename:    sst.Data.Filename,
			StartOffset: sst.Data.StartOffset,
			Size:        sst.Data.Size,
		},
		{
			Filename:    sst.Index.Filename,
			StartOffset: sst.Index.StartOffset,
			Size:        sst.Index.Size,
		},
		{
			Filename:    sst.Summary.Filename,
			StartOffset: sst.Summary.StartOffset,
			Size:        sst.Summary.Size,
		},
		{
			Filename:    sst.Filter.Filename,
			StartOffset: sst.Filter.StartOffset,
			Size:        sst.Filter.Size,
		},
	}
}

// GetNextSStableLabel finds the largest label number in the given path and returns the next label number.
func GetNextSStableLabel(path string) (int, error) {
	// ReadDir if exists. if not, create and read
	folder, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(path, 0755)
			if err != nil {
				return 0, err
			}
		} else {
			return 0, err
		}
	}

	re := regexp.MustCompile(`usertable-(\d+)-TOC.txt`)

	maxNum := 0
	for _, file := range folder {
		if file.IsDir() {
			continue
		}
		match := re.FindStringSubmatch(file.Name())
		if match != nil {
			label, err := strconv.Atoi(match[1])
			if err != nil {
				return 0, err
			}
			if label > maxNum {
				maxNum = label
			}
		}
	}

	return maxNum + 1, nil
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

// Rename renames all SSTable files with the new label number.
// This modifies the position of the SSTable withing the same level of the LSM Tree.
func (sst *SSTable) Rename(label int) error {
	re := regexp.MustCompile(`.*usertable-(\d+)-TOC.txt`)
	match := re.FindStringSubmatch(sst.TOCFilename)
	if match != nil {
		_, err := strconv.Atoi(match[1])
		if err != nil {
			return err
		}
		prefix := match[0][:len(match[0])-len(match[1])-18] // 18 is the length of "usertable--TOC.txt"
		newPath := fmt.Sprintf("%susertable-%05d-TOC.txt", prefix, label)
		err = os.Rename(sst.TOCFilename, newPath)
		if err != nil {
			return err
		}
		sst.TOCFilename = newPath
	} else {
		return errors.New("malformed sstable")
	}

	// rename all other filenames
	re = regexp.MustCompile(`.*usertable-(\d+)-Metadata.txt`)
	match = re.FindStringSubmatch(sst.MetadataFilename)
	if match == nil {
		return errors.New("malformed sstable")
	}
	prefix := match[0][:len(match[0])-len(match[1])-23] // 23 is the length of "usertable--Metadata.txt"

	singleFile := sst.Data.Filename == sst.Index.Filename
	var newDataFilename, newIndexFilename, newSummaryFilename, newFilterFilename string
	if singleFile {
		newDataFilename = fmt.Sprintf("%susertable-%05d-SSTable.db", prefix, label)
		newIndexFilename = fmt.Sprintf("%susertable-%05d-SSTable.db", prefix, label)
		newSummaryFilename = fmt.Sprintf("%susertable-%05d-SSTable.db", prefix, label)
		newFilterFilename = fmt.Sprintf("%susertable-%05d-SSTable.db", prefix, label)
	} else {
		newDataFilename = fmt.Sprintf("%susertable-%05d-Data.db", prefix, label)
		newIndexFilename = fmt.Sprintf("%susertable-%05d-Index.db", prefix, label)
		newSummaryFilename = fmt.Sprintf("%susertable-%05d-Summary.db", prefix, label)
		newFilterFilename = fmt.Sprintf("%susertable-%05d-Filter.db", prefix, label)
	}
	newMetadataFilename := fmt.Sprintf("%susertable-%05d-Metadata.txt", prefix, label)

	err := os.Rename(sst.Data.Filename, newDataFilename)
	if err != nil {
		return err
	}
	sst.Data.Filename = newDataFilename

	err = os.Rename(sst.Index.Filename, newIndexFilename)
	if err != nil {
		return err
	}
	sst.Index.Filename = newIndexFilename

	err = os.Rename(sst.Summary.Filename, newSummaryFilename)
	if err != nil {
		return err
	}
	sst.Summary.Filename = newSummaryFilename

	err = os.Rename(sst.Filter.Filename, newFilterFilename)
	if err != nil {
		return err
	}
	sst.Filter.Filename = newFilterFilename

	err = os.Rename(sst.MetadataFilename, newMetadataFilename)
	if err != nil {
		return err
	}
	sst.MetadataFilename = newMetadataFilename

	// write new toc file
	err = sst.writeTOCFile()
	if err != nil {
		return err
	}

	return nil
}

// OpenSSTableFromToc opens an SSTable from the given TOC file.
func OpenSSTableFromToc(tocPath string) (*SSTable, error) {
	tocFile, err := os.Open(tocPath)
	if err != nil {
		return nil, err
	}
	defer tocFile.Close()

	sstable := &SSTable{
		TOCFilename: tocPath,
	}

	for i := 0; i < 4; i++ {
		var startOffset int64
		var size int64
		var filename string

		_, err := fmt.Fscanf(tocFile, "%d %d %s\n", &startOffset, &size, &filename)
		if err != nil {
			return nil, err
		}

		switch i {
		case 0:
			sstable.Data = DataBlock{
				util.BinaryFile{
					Filename:    filename,
					StartOffset: startOffset,
					Size:        size,
				},
			}
		case 1:
			sstable.Index = IndexBlock{
				util.BinaryFile{
					Filename:    filename,
					StartOffset: startOffset,
					Size:        size,
				},
			}
		case 2:
			sstable.Summary = SummaryBlock{
				BinaryFile: util.BinaryFile{
					Filename:    filename,
					StartOffset: startOffset,
					Size:        size,
				},
			}
		case 3:
			sstable.Filter = FilterBlock{
				BinaryFile: util.BinaryFile{
					Filename:    filename,
					StartOffset: startOffset,
					Size:        size,
				},
			}
		}
	}

	_, err = fmt.Fscanf(tocFile, "%s\n", &sstable.MetadataFilename)
	if err != nil {
		return nil, err
	}

	return sstable, nil
}

// Read returns the record with the given key from the SSTable.
// Returns nil if the key does not exist.
// Returns an error if the read fails.
func (sst *SSTable) Read(key []byte) (*model.Record, error) {
	if !sst.Filter.HasLoaded() {
		err := sst.Filter.Load()
		if err != nil {
			return nil, err
		}
		defer func() {
			sst.Filter.Filter = nil
		}()
	}
	if !sst.Filter.Filter.HasKey(key) {
		return nil, nil
	}

	if !sst.Summary.HasRangeLoaded() {
		err := sst.Summary.LoadRange()
		if err != nil {
			return nil, err
		}
		defer func() {
			sst.Summary.StartKey = nil
			sst.Summary.EndKey = nil
		}()
	}
	if bytes.Compare(key, sst.Summary.StartKey) < 0 {
		return nil, nil
	}
	if bytes.Compare(key, sst.Summary.EndKey) > 0 {
		return nil, nil
	}

	if !sst.Summary.HasLoaded() {
		err := sst.Summary.Load()
		if err != nil {
			return nil, err
		}
		defer func() {
			sst.Summary.Records = nil
		}()
	}
	sr, err := sst.Summary.GetIndexOffset(key)
	if err != nil {
		return nil, err
	}

	ir, err := sst.Index.GetRecordWithKeyFromOffset(key, sr.Offset)
	if err != nil {
		return nil, err
	}

	dr, err := sst.Data.GetRecordWithKeyFromOffset(key, ir.Offset)
	if err != nil {
		return nil, err
	}

	return &model.Record{
		Key:       dr.Key,
		Value:     dr.Value,
		Tombstone: dr.Tombstone,
		Timestamp: dr.Timestamp,
	}, nil
}

// BuildFromDataBlock assumes that data block is correctly created and creates all other components of the SSTable.
func (sst *SSTable) BuildFromDataBlock(numRecords uint, config *util.SSTableConfig) error {
	if config.SingleFile {
		sst.Index.StartOffset = sst.Data.StartOffset + sst.Data.Size
	}
	err := sst.Index.CreateFromDataBlock(config.IndexDegree, &sst.Data)
	if err != nil {
		return err
	}

	if config.SingleFile {
		sst.Summary.StartOffset = sst.Index.StartOffset + sst.Index.Size
	}
	err = sst.Summary.CreateFromIndexBlock(config.SummaryDegree, &sst.Index)
	if err != nil {
		return err
	}

	if config.SingleFile {
		sst.Filter.StartOffset = sst.Summary.StartOffset + sst.Summary.Size
	}
	err = sst.Filter.CreateFromDataBlock(numRecords, config.FilterPrecision, &sst.Data)
	if err != nil {
		return err
	}
	err = sst.Filter.Write()
	if err != nil {
		return err
	}
	sst.Filter.Filter = nil

	files := sst.toBinaryFiles()
	merkleTree := merkle_tree.NewMerkleTree(files, config.MerkleTreeChunkSize)
	file, err := os.Create(sst.MetadataFilename)
	_, err = file.WriteString(merkleTree.Serialize())
	if err != nil {
		return err
	}

	err = sst.writeTOCFile()
	if err != nil {
		return err
	}

	return nil
}
