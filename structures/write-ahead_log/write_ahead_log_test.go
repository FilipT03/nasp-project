package write_ahead_log

import (
	"nasp-project/util"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNewWAL tests the WAL constructor.
func TestNewWAL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpDir)

	config := &util.WALConfig{
		SegmentSize:   128,
		BufferSize:    8,
		WALFolderPath: tmpDir,
	}

	wal, err := NewWAL(config, 100)
	if err != nil {
		t.Errorf("Failed to create Write Ahead Log: %v", err)
	}

	if wal == nil {
		t.Error("Expected Write Ahead Log, got nil")
	}

	// Check for existence of the first log
	logName := "wal_" + strings.Repeat("0", NumberEnd-NumberStart-1) + "1.log"
	logStat, err := os.Stat(filepath.Join(wal.logsPath, logName))
	if err != nil {
		t.Errorf("Failed to stat WAL log: %v", err)
	}
	logSize := logStat.Size()
	if logSize != HeaderSize {
		t.Errorf("Default log should be %v bytes, but it's %v bytes", HeaderSize, logSize)
	}

	// Check for existence of memtable indexing file
	_, err = os.Stat(filepath.Join(wal.memtableIndexingPath))
	if err != nil {
		t.Errorf("Failed to stat WAL memtable indexing: %v", err)
	}
}

// TestWAL_PutCommit tests the WAL.PutCommit function.
func TestWAL_PutCommit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpDir)

	config := &util.WALConfig{
		SegmentSize:   128,
		BufferSize:    8,
		WALFolderPath: tmpDir,
	}

	wal, err := NewWAL(config, 100)
	if err != nil {
		t.Errorf("Failed to create Write Ahead Log: %v", err)
	}

	if wal == nil {
		t.Error("Expected Write Ahead Log, got nil")
	}

	err = wal.PutCommit("key1", []byte("value1"))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}
	err = wal.PutCommit("key2", []byte("value2"))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}

	if wal.buffer == nil || len(wal.buffer) != 2 {
		t.Error("Buffer is nil or wrong length")
	}

	if string(wal.buffer[0].Value) != "value1" ||
		wal.buffer[0].Key != "key1" ||
		wal.buffer[0].Tombstone != false {
		t.Error("First record not committed correctly")
	}
	if string(wal.buffer[1].Value) != "value2" ||
		wal.buffer[1].Key != "key2" ||
		wal.buffer[1].Tombstone != false {
		t.Error("Second record not committed correctly")
	}
}

// TestWAL_DeleteCommit tests the WAL.DeleteCommit function.
func TestWAL_DeleteCommit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpDir)

	config := &util.WALConfig{
		SegmentSize:   128,
		BufferSize:    8,
		WALFolderPath: tmpDir,
	}

	wal, err := NewWAL(config, 100)
	if err != nil {
		t.Errorf("Failed to create Write Ahead Log: %v", err)
	}

	err = wal.DeleteCommit("key1", []byte("value1"))
	if err != nil {
		t.Errorf("Failed to commit Delete: %v", err)
	}
	err = wal.DeleteCommit("key2", []byte("value2"))
	if err != nil {
		t.Errorf("Failed to commit Delete: %v", err)
	}

	if wal.buffer == nil || len(wal.buffer) != 2 {
		t.Error("Buffer is nil or wrong length")
	}

	if string(wal.buffer[0].Value) != "value1" ||
		wal.buffer[0].Key != "key1" ||
		wal.buffer[0].Tombstone != true {
		t.Error("First record not committed correctly")
	}
	if string(wal.buffer[1].Value) != "value2" ||
		wal.buffer[1].Key != "key2" ||
		wal.buffer[1].Tombstone != true {
		t.Error("Second record not committed correctly")
	}
}

// TestWAL_writeBufferBasic tests writing small logs in WAL.
func TestWAL_writeBufferBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpDir)

	config := &util.WALConfig{
		SegmentSize:   128,
		BufferSize:    2,
		WALFolderPath: tmpDir,
	}

	wal, err := NewWAL(config, 100)
	if err != nil {
		t.Errorf("Failed to create Write Ahead Log: %v", err)
	}

	err = wal.PutCommit("key1", []byte("value1"))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}
	err = wal.PutCommit("key2", []byte("value2"))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}

	if len(wal.buffer) != 0 {
		t.Error("Buffer not emptied")
	}

	file, err := os.OpenFile(wal.logsPath+wal.latestFileName, os.O_RDWR, 0644)
	if err != nil {
		t.Errorf("Failed to open log: %v", err)
	}
	fileStat, _ := file.Stat()
	fileSize := fileStat.Size()
	bytes := make([]byte, fileSize)
	_, err = file.Read(bytes)
	if err != nil {
		t.Errorf("Failed to read log: %v", err)
	}

	recs := make([]*Record, 2)
	recs[0], err = wal.readRecordFromSlice(HeaderSize, bytes)
	if err != nil {
		t.Errorf("Failed to read first record from log: %v", err)
	}
	recSize := KeyStart + recs[0].KeySize + recs[0].ValueSize
	recs[1], err = wal.readRecordFromSlice(HeaderSize+recSize, bytes)
	if err != nil {
		t.Errorf("Failed to read second record from log: %v", err)
	}

	expectedRecs := []*Record{
		createRecord("key1", []byte("value1"), false),
		createRecord("key2", []byte("value2"), false),
	}

	if !expectedRecs[0].Equals(recs[0], true) {
		t.Errorf("Expected:\n%s\n"+"Got:\n%s", expectedRecs[0].ToString(), recs[0].ToString())
	}
	if !expectedRecs[1].Equals(recs[1], true) {
		t.Errorf("Expected:\n%s\n"+"Got:\n%s", expectedRecs[1].ToString(), recs[1].ToString())
	}
}

// TestWALReadWrite tests writing records and then reading them.
func TestWALReadWrite(t *testing.T) {

	tmpDir, err := os.MkdirTemp("", "wal_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpDir)

	config := &util.WALConfig{
		SegmentSize:   128,
		BufferSize:    2,
		WALFolderPath: tmpDir,
	}

	wal, err := NewWAL(config, 100)
	if err != nil {
		t.Errorf("Failed to create Write Ahead Log: %v", err)
	}

	err = wal.PutCommit("key1", []byte("value1"))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}
	err = wal.PutCommit("key2", []byte("value2"))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}

	if len(wal.buffer) != 0 {
		t.Error("Buffer not emptied")
	}

	modelRecs, _, _, err := wal.GetAllRecords()
	if err != nil {
		t.Errorf("Failed to get all records: %v", err)
	}
	if len(modelRecs) != 2 {
		t.Errorf("Expected 2 records, got %d", len(modelRecs))
	}

	recs := []*Record{
		createRecord(string(modelRecs[0].Key), modelRecs[0].Value, modelRecs[0].Tombstone),
		createRecord(string(modelRecs[1].Key), modelRecs[1].Value, modelRecs[1].Tombstone),
	}

	expectedRecs := []*Record{
		createRecord("key1", []byte("value1"), false),
		createRecord("key2", []byte("value2"), false),
	}

	if !expectedRecs[0].Equals(recs[0], true) {
		t.Errorf("Expected:\n%s\n"+"Got:\n%s", expectedRecs[0].ToString(), recs[0].ToString())
	}
	if !expectedRecs[1].Equals(recs[1], true) {
		t.Errorf("Expected:\n%s\n"+"Got:\n%s", expectedRecs[1].ToString(), recs[1].ToString())
	}
}

// TestWAL_writeBufferExactSegments tests writing logs of exact size as the segments in WAL.
func TestWAL_writeBufferExactSegments(t *testing.T) {

	tmpDir, err := os.MkdirTemp("", "wal_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpDir)

	config := &util.WALConfig{
		SegmentSize:   128,
		BufferSize:    2,
		WALFolderPath: tmpDir,
	}

	wal, err := NewWAL(config, 100)
	if err != nil {
		t.Errorf("Failed to create Write Ahead Log: %v", err)
	}

	key := "1"
	valueLength := 128 - (HeaderSize + KeyStart + len(key))
	value1 := strings.Repeat("t", valueLength)
	err = wal.PutCommit(key, []byte(value1))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}

	key = "2"
	valueLength = 128 - (HeaderSize + KeyStart + len(key))
	value2 := strings.Repeat("p", valueLength)
	err = wal.PutCommit(key, []byte(value2))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}

	if len(wal.buffer) != 0 {
		t.Error("Buffer not emptied")
	}

	modelRecs, _, _, err := wal.GetAllRecords()
	if err != nil {
		t.Errorf("Failed to get all records: %v", err)
	}
	if len(modelRecs) != 2 {
		t.Errorf("Expected 2 records, got %d", len(modelRecs))
	}

	recs := []*Record{
		createRecord(string(modelRecs[0].Key), modelRecs[0].Value, modelRecs[0].Tombstone),
		createRecord(string(modelRecs[1].Key), modelRecs[1].Value, modelRecs[1].Tombstone),
	}

	expectedRecs := []*Record{
		createRecord("1", []byte(value1), false),
		createRecord("2", []byte(value2), false),
	}

	if !expectedRecs[0].Equals(recs[0], true) {
		t.Errorf("Expected:\n%s\n"+"Got:\n%s", expectedRecs[0].ToString(), recs[0].ToString())
	}
	if !expectedRecs[1].Equals(recs[1], true) {
		t.Errorf("Expected:\n%s\n"+"Got:\n%s", expectedRecs[1].ToString(), recs[1].ToString())
	}
}

// TestWAL_writeBufferLargerRecords tests writing logs larger than the segment in WAL.
func TestWAL_writeBufferLargerRecords(t *testing.T) {

	tmpDir, err := os.MkdirTemp("", "wal_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer func(path string) {
		_ = os.RemoveAll(path)
	}(tmpDir)

	config := &util.WALConfig{
		SegmentSize:   128,
		BufferSize:    3,
		WALFolderPath: tmpDir,
	}

	wal, err := NewWAL(config, 100)
	if err != nil {
		t.Errorf("Failed to create Write Ahead Log: %v", err)
	}

	key := "1"
	valueLength := 500 - (KeyStart + len(key))
	value1 := strings.Repeat("t", valueLength)
	err = wal.PutCommit(key, []byte(value1))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}

	key = "2"
	valueLength = 100 - (KeyStart + len(key))
	value2 := strings.Repeat("p", valueLength)
	err = wal.PutCommit(key, []byte(value2))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}

	key = "3"
	valueLength = 300 - (KeyStart + len(key))
	value3 := strings.Repeat("m", valueLength)
	err = wal.PutCommit(key, []byte(value3))
	if err != nil {
		t.Errorf("Failed to commit Put: %v", err)
	}

	if len(wal.buffer) != 0 {
		t.Error("Buffer not emptied")
	}

	modelRecs, _, _, err := wal.GetAllRecords()
	if err != nil {
		t.Errorf("Failed to get all records: %v", err)
	}
	if len(modelRecs) != 3 {
		t.Errorf("Expected 3 records, got %d", len(modelRecs))
	}

	recs := []*Record{
		createRecord(string(modelRecs[0].Key), modelRecs[0].Value, modelRecs[0].Tombstone),
		createRecord(string(modelRecs[1].Key), modelRecs[1].Value, modelRecs[1].Tombstone),
		createRecord(string(modelRecs[2].Key), modelRecs[2].Value, modelRecs[2].Tombstone),
	}

	expectedRecs := []*Record{
		createRecord("1", []byte(value1), false),
		createRecord("2", []byte(value2), false),
		createRecord("3", []byte(value3), false),
	}

	if !expectedRecs[0].Equals(recs[0], true) {
		t.Errorf("Expected:\n%s\n"+"Got:\n%s", expectedRecs[0].ToString(), recs[0].ToString())
	}
	if !expectedRecs[1].Equals(recs[1], true) {
		t.Errorf("Expected:\n%s\n"+"Got:\n%s", expectedRecs[1].ToString(), recs[1].ToString())
	}
	if !expectedRecs[2].Equals(recs[2], true) {
		t.Errorf("Expected:\n%s\n"+"Got:\n%s", expectedRecs[2].ToString(), recs[2].ToString())
	}
}
