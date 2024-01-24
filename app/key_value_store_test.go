package app

import (
	"fmt"
	"nasp-project/util"
	"os"
	"path"
	"testing"
)

func TestNewKeyValueStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kv_store_test_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	util.GetConfig().SSTable.SavePath = path.Join(tmpDir, "sstable")
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")

	db, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create key-value store: %v", err)
	}

	if db.config == nil {
		t.Errorf("config is nil")
	}

	if db.wal == nil {
		t.Errorf("wal is nil")
	}

	if db.memtables == nil {
		t.Errorf("memtables is nil")
	}

	if db.cache == nil {
		t.Errorf("cache is nil")
	}
}

func TestKeyValueStore_Put(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kv_store_test_put_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Println(tmpDir)

	util.SaveConfig(path.Join(tmpDir, "config.yaml"))

	db, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create key-value store: %v", err)
	}

	key := "key"
	value := []byte("value")

	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}
}

func TestKeyValueStore_Get(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kv_store_test_get_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	util.GetConfig().SSTable.SavePath = path.Join(tmpDir, "sstable")
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")

	db, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create key-value store: %v", err)
	}

	key := "key"
	value := []byte("value")

	err = db.Put(key, value)
	if err != nil {
		t.Fatalf("Failed to put key-value pair: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if string(got) != string(value) {
		t.Errorf("Expected %v, got %v", value, got)
	}
}

func TestKeyValueStore_Get_NonExistentKey(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kv_store_test_get_non_existent_key_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	util.GetConfig().SSTable.SavePath = path.Join(tmpDir, "sstable")
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")

	db, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create key-value store: %v", err)
	}

	key := "key"

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if got != nil {
		t.Errorf("Expected nil, got %v", got)
	}
}

func TestKeyValueStore_Delete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kv_store_test_delete_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	util.GetConfig().SSTable.SavePath = path.Join(tmpDir, "sstable")
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")

	db, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create key-value store: %v", err)
	}

	key := "non_existent_key"

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if got != nil {
		t.Errorf("Expected nil, got %v", got)
	}
}

func TestKeyValueStore_GetRateLimitReached(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kv_store_test_rate_limit_reached_get_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	util.GetConfig().SSTable.SavePath = path.Join(tmpDir, "sstable")
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")

	util.GetConfig().TokenBucket.Interval = 1_000_000 // definitely long enough not to reset during the test

	db, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create key-value store: %v", err)
	}

	for i := 0; i < int(util.GetConfig().TokenBucket.MaxTokenSize); i++ {
		_, err := db.Get("key")
		if err != nil {
			t.Fatalf("Failed to get value: %v", err)
		}
	}

	_, err = db.Get("key")

	if err == nil {
		t.Fatalf("Expected error, got nil")
	}

	if err.Error() != "rate limit reached" {
		t.Fatalf("Expected error 'rate limit reached', got %v", err)
	}
}

func TestKeyValueStore_PutRateLimitReached(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kv_store_test_rate_limit_reached_put_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	util.GetConfig().SSTable.SavePath = path.Join(tmpDir, "sstable")
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")

	util.GetConfig().TokenBucket.Interval = 1_000_000 // definitely long enough not to reset during the test

	db, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create key-value store: %v", err)
	}

	for i := 0; i < int(util.GetConfig().TokenBucket.MaxTokenSize); i++ {
		err := db.Put("key", []byte("value"))
		if err != nil {
			t.Fatalf("Failed to put key-value pair: %v", err)
		}
	}

	err = db.Put("key", []byte("value"))

	if err == nil {
		t.Fatalf("Expected error, got nil")
	}

	if err.Error() != "rate limit reached" {
		t.Fatalf("Expected error 'rate limit reached', got %v", err)
	}
}

func TestKeyValueStore_DeleteRateLimitReached(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kv_store_test_rate_limit_reached_delete_")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	util.GetConfig().SSTable.SavePath = path.Join(tmpDir, "sstable")
	util.GetConfig().WAL.WALFolderPath = path.Join(tmpDir, "wal")

	util.GetConfig().TokenBucket.Interval = 1_000_000 // definitely long enough not to reset during the test

	db, err := NewKeyValueStore()
	if err != nil {
		t.Fatalf("Failed to create key-value store: %v", err)
	}

	for i := 0; i < int(util.GetConfig().TokenBucket.MaxTokenSize); i++ {
		err := db.Delete("key")
		if err != nil {
			t.Fatalf("Failed to delete key-value pair: %v", err)
		}
	}

	err = db.Delete("key")

	if err == nil {
		t.Fatalf("Expected error, got nil")
	}

	if err.Error() != "rate limit reached" {
		t.Fatalf("Expected error 'rate limit reached', got %v", err)
	}
}
