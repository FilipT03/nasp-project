package sstable

import (
	"bytes"
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
	path := filepath.Join(config.SavePath, "L001") // when creating from memory, always save to L001

	label, err := getNextSStableLabel(filepath.Join(path, "TOC"))
	if err != nil {
		return nil, err
	}

	var sstable *SSTable = nil

	if config.SingleFile {
		// Starting offset for each block is calculated after the previous block is written.
		sstable = &SSTable{
			Data: DataBlock{
				Filename:    filepath.Join(path, "usertable-"+label+"-SSTable.db"),
				StartOffset: 0,
			},
			Index: IndexBlock{
				Filename: filepath.Join(path, "usertable-"+label+"-SSTable.db"),
			},
			Summary: SummaryBlock{
				Filename: filepath.Join(path, "usertable-"+label+"-SSTable.db"),
			},
			Filter: FilterBlock{
				Filename: filepath.Join(path, "usertable-"+label+"-SSTable.db"),
			},
			TOCFilename:      filepath.Join(path, "TOC", "usertable-"+label+"-TOC.txt"),
			MetadataFilename: filepath.Join(path, "usertable-"+label+"-Metadata.txt"),
		}
	} else {
		sstable = &SSTable{
			Data: DataBlock{
				Filename:    filepath.Join(path, "usertable-"+label+"-Data.db"),
				StartOffset: 0,
			},
			Index: IndexBlock{
				Filename:    filepath.Join(path, "usertable-"+label+"-Index.db"),
				StartOffset: 0,
			},
			Summary: SummaryBlock{
				Filename:    filepath.Join(path, "usertable-"+label+"-Summary.db"),
				StartOffset: 0,
			},
			Filter: FilterBlock{
				Filename:    filepath.Join(path, "usertable-"+label+"-Filter.db"),
				StartOffset: 0,
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
	// ReadDir if exists. if not, create and read
	folder, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(path, 0755)
			if err != nil {
				return "", err
			}
		} else {
			return "", err
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
				Filename:    filename,
				StartOffset: startOffset,
				Size:        size,
			}
		case 1:
			sstable.Index = IndexBlock{
				Filename:    filename,
				StartOffset: startOffset,
				Size:        size,
			}
		case 2:
			sstable.Summary = SummaryBlock{
				Filename:    filename,
				StartOffset: startOffset,
				Size:        size,
			}
		case 3:
			sstable.Filter = FilterBlock{
				Filename:    filename,
				StartOffset: startOffset,
				Size:        size,
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
func (sst *SSTable) Read(key []byte) (*DataRecord, error) {
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

	record, err := sst.Data.GetRecordWithKeyFromOffset(key, ir.Offset)
	if err != nil {
		return nil, err
	}

	return record, nil
}

// Read searches the LSM tree for the record with the given key.
// Returns the record if it is found, nil otherwise.
// The returned record is from the lowest LSM Tree level that contains the record.
// If the record is found in multiple same-level SSTables, the record with the latest timestamp is returned.
// Returns an error if the read fails.
func Read(key []byte, config util.Config) (*DataRecord, error) {
	for lvl := 1; lvl <= config.LSMTree.MaxLevel; lvl++ {
		lvlLabel := fmt.Sprintf("L%03d", lvl)
		path := filepath.Join(config.SSTable.SavePath, lvlLabel, "TOC")
		folder, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}

		var record *DataRecord = nil
		for _, file := range folder {
			if file.IsDir() {
				continue
			}
			table, err := OpenSSTableFromToc(filepath.Join(path, file.Name()))
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
