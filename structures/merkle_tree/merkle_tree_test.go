package merkle_tree

import (
	"crypto/rand"
	"nasp-project/util"
	"os"
	"path/filepath"
	"testing"
)

func TestMerkleTree(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "merkle_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	files := createTestFiles(t, tmpDir)

	// Test NewMerkleTree function
	var chunkSize int64 = 1024
	merkleTree := NewMerkleTree(files, chunkSize)

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
	anotherMerkleTree := NewMerkleTree(files, chunkSize)
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
func createTestFiles(t *testing.T, tmpDir string) []util.BinaryFile {
	files := []util.BinaryFile{
		{
			filepath.Join(tmpDir, "file1"),
			0,
			10,
		},
		{
			filepath.Join(tmpDir, "file2"),
			0,
			10,
		},
		{
			filepath.Join(tmpDir, "file3"),
			10,
			20,
		},
	}

	// Create the test files
	for _, file := range files {
		f, err := os.Create(file.Filename)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		defer f.Close()
		_, err = f.Seek(file.StartOffset, 0)
		if err != nil {
			t.Fatalf("Failed to seek to the start offset of test file: %v", err)
		}
		_, err = f.Write(generateRandomBytes())
		if err != nil {
			t.Fatalf("Failed to write to test file: %v", err)
		}
	}

	return files
}

func generateRandomBytes() []byte {
	b := make([]byte, 10)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
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
