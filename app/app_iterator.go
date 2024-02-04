package app

import (
	"errors"
	"nasp-project/structures/iterator"
	"nasp-project/structures/lsm"
)

// Iterator through key-value pair records saved in the engine.
type Iterator struct {
	iter *iterator.Iterator
}

// NewIterator creates new Iterator from iterator.Iterator.
func NewIterator(iter *iterator.Iterator) *Iterator {
	return &Iterator{iter: iter}
}

// Next returns the next key-value pair from the iterator.
func (it *Iterator) Next() (key string, val []byte) {
	rec := it.iter.Next()
	for rec != nil && rec.Tombstone {
		rec = it.iter.Next()
	}
	if rec == nil {
		return "", nil
	}
	return string(rec.Key), rec.Value
}

// Stop stops end invalidates the iterator. Every subsequent call to Next return nil.
func (it *Iterator) Stop() {
	it.iter.Stop()
}

// RangeIterate returns an Iterator that iterates through records with key in range [minKey, maxKey].
func (kvs *KeyValueStore) RangeIterate(minKey, maxKey string) (*Iterator, error) {
	if block, err := kvs.rateLimitReached(); block {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("rate limit reached")
	}

	compressionDict, err := kvs.getCompressionDict("") // dict with all keys loaded
	if err != nil {
		return nil, err
	}

	iters := kvs.memtables.GetRangeIterators([]byte(minKey), []byte(maxKey))

	sstIters, err := lsm.GetRangeIterators([]byte(minKey), []byte(maxKey), compressionDict, kvs.config)
	if err != nil {
		return nil, err
	}
	iters = append(iters, sstIters...)

	iter, err := iterator.NewIterator(iters)
	if err != nil {
		return nil, err
	}

	return NewIterator(iter), nil
}

// PrefixIterate returns an Iterator that iterates through records with a given key prefix.
func (kvs *KeyValueStore) PrefixIterate(prefix string) (*Iterator, error) {
	if block, err := kvs.rateLimitReached(); block {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("rate limit reached")
	}

	compressionDict, err := kvs.getCompressionDict("") // dict with all keys loaded
	if err != nil {
		return nil, err
	}

	iters := kvs.memtables.GetPrefixIterators([]byte(prefix))

	sstIters, err := lsm.GetPrefixIterators([]byte(prefix), compressionDict, kvs.config)
	if err != nil {
		return nil, err
	}
	iters = append(iters, sstIters...)

	iter, err := iterator.NewIterator(iters)
	if err != nil {
		return nil, err
	}

	return NewIterator(iter), nil
}
