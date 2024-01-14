package size_tiered_compaction

import (
	"fmt"
	"nasp-project/model"
	"nasp-project/structures/sstable"
	"nasp-project/util"
	"os"
	"testing"
)

func TestCompact(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := &util.SSTableConfig{
		SavePath:            tmpDir,
		SingleFile:          false,
		IndexDegree:         2,
		SummaryDegree:       3,
		FilterPrecision:     0.01,
		MerkleTreeChunkSize: 16,
	}

	// Create some sample data records.
	recs := []model.Record{
		{Key: []byte("key1"), Value: []byte("value1"), Timestamp: 1},
		{Key: []byte("key2"), Value: []byte("value2"), Timestamp: 2},
	}

	sstable1, err := sstable.CreateSSTable(recs, config)
	if err != nil {
		fmt.Println(sstable1)
	}
	sstable2, err := sstable.CreateSSTable(recs, config)
	if err != nil {
		fmt.Println(sstable2)
	}
	sstable3, err := sstable.CreateSSTable(recs, config)
	if err != nil {
		fmt.Println(sstable3)
	}
	sstable4, err := sstable.CreateSSTable(recs, config)
	if err != nil {
		fmt.Println(sstable4)
	}
	lsmConfig := &util.LSMTreeConfig{
		MaxLevel:            3,
		MaxLsmNodesPerLevel: 2,
	}
	Compact(config, lsmConfig)
	// Check if compaction has been performed correctly

	// Example assertion: Check if the number of SSTables in the first level is as expected after compaction.
	pathToToc := tmpDir + "/L001/TOC"
	fileNames := FindSSTables(pathToToc)
	expectedSSTables := 1
	if len(fileNames) != expectedSSTables {
		t.Errorf("Expected %d SSTables after compaction, but got %d", expectedSSTables, len(fileNames))
	}
	// check the L002 level
	pathToToc = tmpDir + "/L002/TOC"
	fileNames = FindSSTables(pathToToc)
	expectedSSTables = 1
	if len(fileNames) != expectedSSTables {
		t.Errorf("Expected %d SSTables after compaction, but got %d", expectedSSTables, len(fileNames))
	}
	// check the L003 level
	pathToToc = tmpDir + "/L003/TOC"
	fileNames = FindSSTables(pathToToc)
	expectedSSTables = 0
	if len(fileNames) != expectedSSTables {
		t.Errorf("Expected %d SSTables after compaction, but got %d", expectedSSTables, len(fileNames))
	}
	// console output
	fmt.Println("We have created 4 sstables and after the compacting we have:")
	fmt.Println("L001: ", len(FindSSTables(tmpDir+"/L001/TOC")))
	fmt.Println("L002: ", len(FindSSTables(tmpDir+"/L002/TOC")))
	fmt.Println("L003: ", len(FindSSTables(tmpDir+"/L003/TOC")))

}
