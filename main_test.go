package main

import (
	"math/rand"
	"nasp-project/app"
	"nasp-project/util"
	"testing"
	"time"
)

var KEY_LENGTH = 20
var VALUE_LENGTH = 50
var NUM_RECORDS = 100000
var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func BenchmarkInsert100(b *testing.B) {
	util.GetConfig().SSTable.Compression = false
	// count disc space
	// create n random keys
	keys := generateKey(100)
	// create keyValueStore
	kvs, _ := app.NewKeyValueStore()

	// put elements
	for i := 0; i < NUM_RECORDS; i++ {
		kvs.Put(keys[randomIdx(100)], generateNewValue())
	}

}

func BenchmarkInsertWithCompression100(b *testing.B) {
	util.GetConfig().SSTable.Compression = true
	// count disc space
	// create n random keys
	keys := generateKey(100)
	// create keyValueStore
	kvs, _ := app.NewKeyValueStore()

	// put elements
	for i := 0; i < NUM_RECORDS; i++ {
		kvs.Put(keys[randomIdx(100)], generateNewValue())
	}

}

func BenchmarkInsertWithCompression50000(b *testing.B) {
	util.GetConfig().SSTable.Compression = true
	// count disc space
	// create n random keys
	keys := generateKey(50000)
	// create keyValueStore
	kvs, _ := app.NewKeyValueStore()

	// put elements
	for i := 0; i < NUM_RECORDS; i++ {
		kvs.Put(keys[randomIdx(100)], generateNewValue())
	}

}

func BenchmarkInsert50000(b *testing.B) {
	util.GetConfig().SSTable.Compression = false
	// count disc space
	// create n random keys
	keys := generateKey(50000)
	// create keyValueStore
	kvs, _ := app.NewKeyValueStore()

	// put elements
	for i := 0; i < NUM_RECORDS; i++ {
		kvs.Put(keys[randomIdx(100)], generateNewValue())
	}

}

func generateNewValue() []byte {
	var value []byte
	for i := 0; i < VALUE_LENGTH; i++ {
		b := make([]byte, VALUE_LENGTH)
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return value
}

func randomIdx(len int) int {
	return seededRand.Intn(len)
}

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
