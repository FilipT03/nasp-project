package merkle_tree

import (
	"crypto/rand"
	"nasp-project/model"
	"nasp-project/structures/sstable"
	"nasp-project/util"
	"os"
	"testing"
)

func TestMerkleTree(t *testing.T) {
	// Create a test SSTable
	ssTable := createTestSSTable(t)

	// Test NewMerkleTree function
	chunkSize := 1024
	merkleTree := NewMerkleTree(ssTable, chunkSize)

	// Test Contains method
	if !merkleTree.Contains(merkleTree.root) {
		t.Error("Merkle tree should contain its root")
	}

	// Generate a random hash not present in the Merkle tree
	randomHash := generateRandomHash()
	if merkleTree.Contains(randomHash) {
		t.Error("Merkle tree should not contain a random hash")
	}

	// Test Equal method
	// Create another Merkle tree with the same data
	anotherMerkleTree := NewMerkleTree(ssTable, chunkSize)
	differentNodes := merkleTree.Equal(anotherMerkleTree)
	if len(differentNodes) != 0 {
		t.Error("Merkle trees should be equal, but found differences:", differentNodes)
	}

	// Modify one of the nodes in the second Merkle tree
	anotherMerkleTree.root = Node{left: EmptyBlock{}, right: anotherMerkleTree.root.(Node).right}
	differentNodes = merkleTree.Equal(anotherMerkleTree)
	if len(differentNodes) == 0 {
		t.Error("Merkle trees should be different, but found no differences")
	}
}

// Helper function to create a test SSTable
func createTestSSTable(t *testing.T) sstable.SSTable {
	tmpDir, err := os.MkdirTemp("", "sstable_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	// defer os.RemoveAll(tmpDir)

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

	sstable, err := sstable.CreateSSTable(recs, config)
	if err != nil {
		t.Fatalf("Failed to create SSTable: %v", err)
	}
	return *sstable
}

// Helper function to generate a random hash
func generateRandomHash() Hash {
	var randomHash Hash
	_, err := rand.Read(randomHash[:])
	if err != nil {
		panic(err)
	}
	return randomHash
}
