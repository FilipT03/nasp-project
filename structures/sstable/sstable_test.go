package sstable

import (
	"bytes"
	"nasp-project/model"
	"nasp-project/util"
	"os"
	"path/filepath"
	"testing"
)

// TestCreateSSTable tests the creation of an SSTable with single file configuration.
func TestCreateSSTableSingleFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := util.SSTableConfig{
		SavePath:        tmpDir,
		SingleFile:      true,
		IndexDegree:     2,
		SummaryDegree:   3,
		FilterPrecision: 0.01,
	}

	// Create some sample data records.
	recs := []model.Record{
		{Key: []byte("key1"), Value: []byte("value1"), Timestamp: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Timestamp: 2},
	}

	sstable, err := CreateSSTable(recs, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	if sstable == nil {
		t.Error("Expected SSTable, got nil")
	}

	// check for existence of TOC file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "TOC", "usertable-00001-TOC.txt"))
	if err != nil {
		t.Errorf("Failed to stat TOC file: %v", err)
	}

	// check for existence of a single sstable file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00001-SSTable.db"))
	if err != nil {
		t.Errorf("Failed to stat sstable file: %v", err)
	}
}

// TestCreateSSTable tests the creation of an SSTable with multi file configuration.
func TestCreateSSTableMultiFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := util.SSTableConfig{
		SavePath:        tmpDir,
		SingleFile:      false,
		IndexDegree:     2,
		SummaryDegree:   3,
		FilterPrecision: 0.01,
	}

	// Create some sample data records.
	recs := []model.Record{
		{Key: []byte("key1"), Value: []byte("value1"), Timestamp: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Timestamp: 2},
	}

	sstable, err := CreateSSTable(recs, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	if sstable == nil {
		t.Error("Expected SSTable, got nil")
	}

	// check for existence of TOC file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "TOC", "usertable-00001-TOC.txt"))
	if err != nil {
		t.Errorf("Failed to stat TOC file: %v", err)
	}

	// check for existence of Data file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00001-Data.db"))
	if err != nil {
		t.Errorf("Failed to stat data file: %v", err)
	}

	// check for existence of Index file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00001-Index.db"))
	if err != nil {
		t.Errorf("Failed to stat index file: %v", err)
	}

	// check for existence of Summary file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00001-Summary.db"))
	if err != nil {
		t.Errorf("Failed to stat summary file: %v", err)
	}

	// check for existence of Filter file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00001-Filter.db"))
	if err != nil {
		t.Errorf("Failed to stat filter file: %v", err)
	}
}

// TestCreateSSTableSecond tests the creation of a second SSTable.
func TestCreateSSTableSecond(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := util.SSTableConfig{
		SavePath:        tmpDir,
		SingleFile:      false,
		IndexDegree:     2,
		SummaryDegree:   3,
		FilterPrecision: 0.01,
	}

	// Create some sample data records.
	recs := []model.Record{
		{Key: []byte("key1"), Value: []byte("value1"), Timestamp: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Timestamp: 2},
	}

	_, err = CreateSSTable(recs, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	sstable2, err := CreateSSTable(recs, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	if sstable2 == nil {
		t.Error("Expected SSTable, got nil")
	}

	// check for existence of TOC file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "TOC", "usertable-00002-TOC.txt"))
	if err != nil {
		t.Errorf("Failed to stat TOC file: %v", err)
	}

	// check for existence of Data file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00002-Data.db"))
	if err != nil {
		t.Errorf("Failed to stat data file: %v", err)
	}

	// check for existence of Index file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00002-Index.db"))
	if err != nil {
		t.Errorf("Failed to stat index file: %v", err)
	}

	// check for existence of Summary file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00002-Summary.db"))
	if err != nil {
		t.Errorf("Failed to stat summary file: %v", err)
	}

	// check for existence of Filter file
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00002-Filter.db"))
	if err != nil {
		t.Errorf("Failed to stat filter file: %v", err)
	}
}

// TestOpenSSTableFromTOC tests opening an SSTable from the TOC file.
func TestOpenSSTableFromTOC(t *testing.T) {
	// Create a temporary directory for testing.
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := util.SSTableConfig{
		SavePath:        tmpDir,
		SingleFile:      false,
		IndexDegree:     2,
		SummaryDegree:   3,
		FilterPrecision: 0.01,
	}

	// Create some sample data records.
	recs := []model.Record{
		{Key: []byte("key1"), Value: []byte("value1"), Timestamp: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Timestamp: 2},
	}

	// Create an SSTable.
	original, err := CreateSSTable(recs, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	// Open the SSTable from the TOC file.
	opened, err := OpenSSTableFromToc(filepath.Join(tmpDir, "L001", "TOC", "usertable-00001-TOC.txt"))
	if err != nil {
		t.Errorf("Failed to open SSTable: %v", err)
	}

	// Check that the SSTables are equal.
	if opened.Data.Filename != original.Data.Filename ||
		opened.Data.StartOffset != original.Data.StartOffset ||
		opened.Data.Size != original.Data.Size {
		t.Errorf("Expected data blocks to be equal.")
	}

	if opened.Index.Filename != original.Index.Filename ||
		opened.Index.StartOffset != original.Index.StartOffset ||
		opened.Index.Size != original.Index.Size {
		t.Errorf("Expected index blocks to be equal.")
	}

	if opened.Summary.Filename != original.Summary.Filename ||
		opened.Summary.StartOffset != original.Summary.StartOffset ||
		opened.Summary.Size != original.Summary.Size {
		t.Errorf("Expected summary blocks to be equal.")
	}

	if opened.Filter.Filename != original.Filter.Filename ||
		opened.Filter.StartOffset != original.Filter.StartOffset ||
		opened.Filter.Size != original.Filter.Size {
		t.Errorf("Expected filter blocks to be equal.")
	}

	if opened.MetadataFilename != original.MetadataFilename {
		t.Errorf("Expected metadata filenames to be equal.")
	}
}

// TestDeleteFiles tests deleting the files of an SSTable.
func TestDeleteFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := util.SSTableConfig{
		SavePath:        tmpDir,
		SingleFile:      false,
		IndexDegree:     2,
		SummaryDegree:   3,
		FilterPrecision: 0.01,
	}

	// Create some sample data records.
	recs := []model.Record{
		{Key: []byte("key1"), Value: []byte("value1"), Timestamp: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Timestamp: 2},
	}

	sstable, err := CreateSSTable(recs, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	// Delete the files of the SSTable.
	err = sstable.deleteFiles()
	if err != nil {
		t.Errorf("Error while deleting files: %v", err)
	}

	// Check that the files were deleted.
	_, err = os.Stat(filepath.Join(tmpDir, "L001", "TOC", "usertable-00001-TOC.txt"))
	if !os.IsNotExist(err) {
		t.Errorf("Expected TOC file to be deleted.")
	}

	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00001-Data.db"))
	if !os.IsNotExist(err) {
		t.Errorf("Expected data file to be deleted.")
	}

	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00001-Index.db"))
	if !os.IsNotExist(err) {
		t.Errorf("Expected index file to be deleted.")
	}

	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00001-Summary.db"))
	if !os.IsNotExist(err) {
		t.Errorf("Expected summary file to be deleted.")
	}

	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00001-Filter.db"))
	if !os.IsNotExist(err) {
		t.Errorf("Expected filter file to be deleted.")
	}

	_, err = os.Stat(filepath.Join(tmpDir, "L001", "usertable-00001-Metadata.txt"))
	if !os.IsNotExist(err) {
		t.Errorf("Expected metadata file to be deleted.")
	}
}

// TestSSTable_Read tests reading from an SSTable.
func TestSSTable_Read(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := util.SSTableConfig{
		SavePath:        tmpDir,
		SingleFile:      false,
		IndexDegree:     2,
		SummaryDegree:   3,
		FilterPrecision: 0.01,
	}

	// Create some sample data records.
	recs := []model.Record{
		{Key: []byte("key1"), Value: []byte("value1"), Timestamp: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Timestamp: 2},
	}

	sstable, err := CreateSSTable(recs, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	dr, err := sstable.Read([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if bytes.Compare(dr.Value, []byte("value1")) != 0 {
		t.Errorf("Expected value of 'value1', got %v", dr.Value)
	}

	dr, err = sstable.Read([]byte("key2"))
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if bytes.Compare(dr.Value, []byte("value2")) != 0 {
		t.Errorf("Expected value of 'value2', got %v", dr.Value)
	}
}

// TestMergeSSTables tests merging two SSTables.
func TestMergeSSTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := util.SSTableConfig{
		SavePath:        tmpDir,
		SingleFile:      false,
		IndexDegree:     2,
		SummaryDegree:   3,
		FilterPrecision: 0.01,
	}

	// Create some sample data records.
	recs1 := []model.Record{
		{Key: []byte("key1"), Value: []byte("value1"), Timestamp: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Timestamp: 2},
	}

	recs2 := []model.Record{
		{Key: []byte("key3"), Value: []byte("value3"), Timestamp: 3},
		{Key: []byte("key4"), Value: []byte("value4"), Timestamp: 4},
	}

	// Create two SSTables.
	sstable1, err := CreateSSTable(recs1, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	sstable2, err := CreateSSTable(recs2, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	// Merge the SSTables.
	merged, err := MergeSSTables(sstable1, sstable2, 2, config)
	if err != nil {
		t.Errorf("Failed to merge SSTables: %v", err)
	}

	// Check that the merged SSTable is correct.
	dr, err := merged.Read([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if bytes.Compare(dr.Value, []byte("value1")) != 0 {
		t.Errorf("Expected value of 'value1', got %v", dr.Value)
	}

	dr, err = merged.Read([]byte("key2"))
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if bytes.Compare(dr.Value, []byte("value2")) != 0 {
		t.Errorf("Expected value of 'value2', got %v", dr.Value)
	}

	dr, err = merged.Read([]byte("key3"))
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if bytes.Compare(dr.Value, []byte("value3")) != 0 {
		t.Errorf("Expected value of 'value3', got %v", dr.Value)
	}

	dr, err = merged.Read([]byte("key4"))
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if bytes.Compare(dr.Value, []byte("value4")) != 0 {
		t.Errorf("Expected value of 'value4', got %v", dr.Value)
	}
}

// TestMergeSSTablesSameKey tests merging two SSTables with the same key.
func TestMergeSSTablesSameKey(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := util.SSTableConfig{
		SavePath:        tmpDir,
		SingleFile:      false,
		IndexDegree:     2,
		SummaryDegree:   3,
		FilterPrecision: 0.01,
	}

	// Create some sample data records.
	recs1 := []model.Record{
		{Key: []byte("key1"), Value: []byte("value1"), Timestamp: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Timestamp: 2},
	}

	recs2 := []model.Record{
		{Key: []byte("key2"), Value: []byte("value3"), Timestamp: 3},
		{Key: []byte("key3"), Value: []byte("value4"), Timestamp: 4},
	}

	// Create two SSTables.
	sstable1, err := CreateSSTable(recs1, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	sstable2, err := CreateSSTable(recs2, config)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	// Merge the SSTables.
	merged, err := MergeSSTables(sstable1, sstable2, 2, config)
	if err != nil {
		t.Errorf("Failed to merge SSTables: %v", err)
	}

	dr, err := merged.Read([]byte("key2"))
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if bytes.Compare(dr.Value, []byte("value3")) != 0 {
		t.Errorf("Expected value of 'value2', got %v", dr.Value)
	}
}
