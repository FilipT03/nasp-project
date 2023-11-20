package lsm

import (
	"bytes"
	"nasp-project/model"
	"nasp-project/structures/sstable"
	"nasp-project/util"
	"os"
	"testing"
)

func TestRead(t *testing.T) {
	// Create 3 SSTables.
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sstConfig := util.SSTableConfig{
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
		{Key: []byte("key2"), Value: []byte("value22"), Timestamp: 3},
		{Key: []byte("key3"), Value: []byte("value3"), Timestamp: 4},
	}

	recs3 := []model.Record{
		{Key: []byte("key3"), Value: []byte("value33"), Timestamp: 5},
		{Key: []byte("key4"), Value: []byte("value4"), Timestamp: 6},
	}

	sstable1, err := sstable.CreateSSTable(recs1, sstConfig)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	sstable2, err := sstable.CreateSSTable(recs2, sstConfig)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	// Merge the SSTables 1 and 2 and save to LSM Level 2
	_, err = sstable.MergeSSTables(sstable1, sstable2, 2, sstConfig)
	if err != nil {
		t.Errorf("Failed to merge SSTables: %v", err)
	}

	// Create SSTable 3 on LSM Level 1
	_, err = sstable.CreateSSTable(recs3, sstConfig)
	if err != nil {
		t.Errorf("Failed to create SSTable: %v", err)
	}

	config := &util.Config{
		SSTable: sstConfig,
		LSMTree: util.LSMTreeConfig{
			MaxLevel: 3,
		},
	}
	dr, err := Read([]byte("key1"), config)
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if bytes.Compare(dr.Value, []byte("value1")) != 0 {
		t.Errorf("Expected value of 'value1', got %v", dr.Value)
	}

	dr, err = Read([]byte("key2"), config)
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if bytes.Compare(dr.Value, []byte("value22")) != 0 {
		t.Errorf("Expected value of 'value22', got %v", dr.Value)
	}

	dr, err = Read([]byte("key3"), config)
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}
	if bytes.Compare(dr.Value, []byte("value33")) != 0 {
		t.Errorf("Expected value of 'value33', got %v", dr.Value)
	}
}
