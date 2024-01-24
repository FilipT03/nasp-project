package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"nasp-project/app"
	"nasp-project/structures/sstable"
	"nasp-project/util"
	"os"
	"path"
	"testing"
	"time"
)

// Key and value length for the inserted records.
var KEY_LENGTH = 20
var VALUE_LENGTH = 50

// Number of records to insert.
var NUM_RECORDS = 100000

// Generate a ranodm seed.
var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

// charset used for generating random keys and values.
const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// BenchmarkInsert100 the insertion of 100000 records with 100 keys.
// the test count the space occupied by the SSTables
func BenchmarkInsert100(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "sstable")
	if err != nil {
		b.Error(err)
	}

	defer os.RemoveAll(tmpDir)

	// set confing file for testing
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")
	config := util.GetConfig()
	config.SSTable.SavePath = tmpDir
	config.SSTable.Compression = false
	// set memtable size to 50 because I insert only 100 records
	config.Memtable.MaxSize = 50
	filepath := config.SSTable.SavePath
	levels := config.LSMTree.MaxLevel

	// count disc space
	var occupiedSize int64 = 0

	// create n random keys
	keys := generateKey(100)

	// create keyValueStore
	kvs, _ := app.NewKeyValueStore(config)

	// put elements in the key value store
	for i := 0; i < NUM_RECORDS; i++ {
		kvs.Put(keys[randomIdx(100)], generateNewValue())
	}
	// go through all the levels and count the occupied space
	// for each level by summing the size of the SSTables
	for i := 1; i <= levels; i++ {
		pathToToc := filepath + "/L" + fmt.Sprintf("%03d", i) + "/TOC"
		fileNames := FindSSTables(pathToToc)

		// for each SSTable in the current level
		// open it and add its size to the occupiedSize
		for j := 0; j < len(fileNames); j++ {
			currSSTable, err := sstable.OpenSSTableFromToc(pathToToc + "/" + fileNames[j])
			if err != nil {
				b.Error(err)
			}
			occupiedSize += currSSTable.Size()
		}
	}
	fmt.Println("Occupied size: ", occupiedSize, " bytes")

}

// BenchmarkInsert100 the insertion of 100000 records with 100 keys with compression set to true.
// the test count the space occupied by the SSTables
func BenchmarkInsertWithCompression100(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "sstable")
	if err != nil {
		return
	}
	defer os.RemoveAll(tmpDir)

	util.GetConfig().SSTable.SavePath = tmpDir
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")
	config := util.GetConfig()
	config.SSTable.Compression = true
	// set memtable size to 50 because I insert only 100 records
	config.Memtable.MaxSize = 50
	// count disc space
	// create n random keys
	keys := generateKey(100)
	// create keyValueStore
	kvs, _ := app.NewKeyValueStore(config)
	var occupiedSize int64 = 0
	filepath := config.SSTable.SavePath
	levels := config.LSMTree.MaxLevel

	// put elements
	for i := 0; i < NUM_RECORDS; i++ {
		kvs.Put(keys[randomIdx(100)], generateNewValue())
	}
	for i := 1; i <= levels; i++ {
		pathToToc := filepath + "/L" + fmt.Sprintf("%03d", i) + "/TOC"
		fileNames := FindSSTables(pathToToc)
		for j := 0; j < len(fileNames); j++ {
			currSSTable, err := sstable.OpenSSTableFromToc(pathToToc + "/" + fileNames[j])
			if err != nil {
				b.Error(err)
			}
			occupiedSize += currSSTable.Size()
		}
	}
	fmt.Println("Occupied size: ", occupiedSize, " bytes")

}

// BenchmarkInsert50000 the insertion of 100000 records with 50000 keys.
// the test count the space occupied by the SSTables
func BenchmarkInsertWithCompression50000(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "sstable")
	if err != nil {
		return
	}
	defer os.RemoveAll(tmpDir)

	util.GetConfig().SSTable.SavePath = tmpDir
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")
	config := util.GetConfig()
	config.Memtable.MaxSize = 50
	config.SSTable.Compression = true
	// count disc space
	// create n random keys
	keys := generateKey(50000)
	// create keyValueStore
	kvs, _ := app.NewKeyValueStore(config)
	var occupiedSize int64 = 0
	filepath := config.SSTable.SavePath
	levels := config.LSMTree.MaxLevel

	// put elements
	for i := 0; i < NUM_RECORDS; i++ {
		kvs.Put(keys[randomIdx(50000)], generateNewValue())
	}
	for i := 1; i <= levels; i++ {
		pathToToc := filepath + "/L" + fmt.Sprintf("%03d", i) + "/TOC"
		fileNames := FindSSTables(pathToToc)
		for j := 0; j < len(fileNames); j++ {
			currSSTable, err := sstable.OpenSSTableFromToc(pathToToc + "/" + fileNames[j])
			if err != nil {
				b.Error(err)
			}
			occupiedSize += currSSTable.Size()
		}
	}
	fmt.Println("Occupied size: ", occupiedSize, " bytes")

}

// BenchmarkInsert50000 the insertion of 100000 records with 50000 keys with compression set to true.
// the test count the space occupied by the SSTables
func BenchmarkInsert50000(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "sstable")
	if err != nil {
		return
	}
	defer os.RemoveAll(tmpDir)

	util.GetConfig().SSTable.SavePath = tmpDir
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")
	config := util.GetConfig()
	config.SSTable.Compression = false
	config.Memtable.MaxSize = 50
	// count disc space
	// create n random keys
	keys := generateKey(50000)
	// create keyValueStore
	kvs, _ := app.NewKeyValueStore(config)
	var occupiedSize int64 = 0
	filepath := config.SSTable.SavePath
	levels := config.LSMTree.MaxLevel

	// put elements
	for i := 0; i < NUM_RECORDS; i++ {
		kvs.Put(keys[randomIdx(50000)], generateNewValue())
	}
	for i := 1; i <= levels; i++ {
		pathToToc := filepath + "/L" + fmt.Sprintf("%03d", i) + "/TOC"
		fileNames := FindSSTables(pathToToc)
		for j := 0; j < len(fileNames); j++ {
			currSSTable, err := sstable.OpenSSTableFromToc(pathToToc + "/" + fileNames[j])
			if err != nil {
				b.Error(err)
			}
			occupiedSize += currSSTable.Size()
		}
	}
	fmt.Println("Occupied size: ", occupiedSize, " bytes")

}

// FindSSTables returns the names of the SSTables in the given directory.
func FindSSTables(filepath string) []string {
	// read the directory
	files, _ := ioutil.ReadDir(filepath)
	// used for storing the names of the SSTables
	var sstableNames []string
	// for each filename in directory I want to append it to the sstableNames
	for _, file := range files {
		sstableNames = append(sstableNames, file.Name())
	}

	return sstableNames
}

// generateNewValue generates a random value of length VALUE_LENGTH.
func generateNewValue() []byte {
	b := make([]byte, VALUE_LENGTH)
	for i := 0; i < VALUE_LENGTH; i++ {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return b
}

// randomIdx returns a random index in the range [0, len).
func randomIdx(len int) int {
	return seededRand.Intn(len)
}

// generateKey generates a random key of length KEY_LENGTH.
func generateKey(count int) []string {
	var keys []string
	for i := 0; i < count; i++ {
		b := make([]byte, KEY_LENGTH)
		for i := range b {
			b[i] = charset[seededRand.Intn(len(charset))]
		}
		keys = append(keys, string(b))
	}
	return keys

}
