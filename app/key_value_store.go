package app

import (
	"nasp-project/model"
	"nasp-project/structures/lru_cache"
	"nasp-project/structures/lsm"
	"nasp-project/structures/memtable"
	"nasp-project/structures/sstable"
	writeaheadlog "nasp-project/structures/write-ahead_log"
	"nasp-project/util"
	"time"
)

const ConfigPath = "config.yaml"

type KeyValueStore struct {
	config    *util.Config
	wal       *writeaheadlog.WAL
	memtables *memtable.Memtables
	cache     *lru_cache.LRUCache
}

// NewKeyValueStore creates an instance of Key-Value Storage engine with configuration given at ConfigPath.
func NewKeyValueStore() (*KeyValueStore, error) {
	config := util.LoadConfig(ConfigPath)

	wal, err := writeaheadlog.NewWAL(config.WAL.BufferSize, config.WAL.SegmentSize)
	if err != nil {
		return nil, err
	}

	mts := memtable.CreateMemtables(&config.Memtable)
	// TODO: Load WAL records into memtables

	cache := lru_cache.NewLRUCache(config.Cache.MaxSize)

	return &KeyValueStore{
		config:    config,
		wal:       wal,
		memtables: mts,
		cache:     &cache,
	}, nil
}

// Get returns a value associated with the specified key from the database.
// Returns nil if the key is not found.
// Returns an error if the read fails.
func (kvs *KeyValueStore) Get(key string) ([]byte, error) {
	return kvs.get(key)
}

// Put saves a key-value pair to the database.
// Returns an error if the write fails.
func (kvs *KeyValueStore) Put(key string, value []byte) error {
	return kvs.put(key, value)
}

// Delete deletes a value associated with the specified key from the database.
// Returns an error if the write fails.
func (kvs *KeyValueStore) Delete(key string) error {
	return kvs.delete(key)
}

// get returns a value associated with the specified key from the database.
// Implements complete read-path: Memtable -> Cache -> SSTable
// Returns nil if the key is not found.
// Returns an error if the read fails.
func (kvs *KeyValueStore) get(key string) ([]byte, error) {
	keyBytes := []byte(key)

	rec, err := kvs.memtables.Get(keyBytes)
	if err == nil && rec != nil {
		if rec.Tombstone {
			return nil, nil
		}
		return rec.Value, nil
	}

	rec = kvs.cache.Get(key)
	if rec != nil {
		if rec.Tombstone {
			return nil, nil
		}
		return rec.Value, nil
	}

	rec, err = lsm.Read(keyBytes, kvs.config)
	if err != nil {
		return rec.Value, err
	}

	kvs.cache.Put(rec)

	if rec.Tombstone {
		return nil, nil
	}
	return rec.Value, nil
}

// put saves a key-value pair to the database.
// The record is guaranteed to be saved in the memtable.
// If the Memtable is full it flushes its contents into SSTable.
// A flush can trigger an LSM Tree compaction if the condition is met.
// Returns an error if the write fails.
func (kvs *KeyValueStore) put(key string, value []byte) error {
	record := &model.Record{
		Key:       []byte(key),
		Value:     value,
		Tombstone: false,
		Timestamp: uint64(time.Now().Unix()),
	}

	kvs.wal.PutCommit(key, value)

	if kvs.memtables.IsFull() {
		recs := kvs.memtables.Flush()

		_, err := sstable.CreateSSTable(recs, &kvs.config.SSTable)
		if err != nil {
			return err
		}

		err = lsm.Compact(&kvs.config.LSMTree, &kvs.config.SSTable)
		if err != nil {
			return err
		}

		// TODO: Delete WAL records

		for _, rec := range recs {
			if kvs.cache.Get(string(rec.Key)) != nil {
				kvs.cache.Put(&rec)
			}
		}
	}

	err := kvs.memtables.Add(record)
	if err != nil {
		return err
	}

	return nil
}

// delete preforms a logic delete of the key-value pair.
// If the record is found in the Memtable, its Tombstone field is set.
// If not found in Memtable, a new record with set Tombstone is created.
// Uses put operation for adding a new record.
// Returns an error if the write fails.
func (kvs *KeyValueStore) delete(key string) error {
	kvs.wal.DeleteCommit(key, nil)

	err := kvs.memtables.Delete([]byte(key)) // sets the tombstone to true
	if err != nil {
		// key does not exist in memtables, add it first
		err = kvs.put(key, nil)
		if err != nil {
			return err
		}
		// it is now guaranteed to be in a memtable
		return kvs.memtables.Delete([]byte(key))
	}
	return nil
}
