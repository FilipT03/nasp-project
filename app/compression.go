package app

import "nasp-project/structures/compression"

const CompressionDictKey = "__COMP_DICT__"

// getCompressionDict returns the current global compression dictionary.
// If the compression is turned off, returns nil.
// requestKey parameter signals that kvs.get(requestKey) was called and requested a compression dictionary.
// If the value of requestKey is CompressionDictKey no additional operation is performed on the engine
// and a new compression dictionary containing only CompressionDictKey mapping is returned.
func (kvs *KeyValueStore) getCompressionDict(requestKey string) (*compression.Dictionary, error) {
	if !kvs.config.SSTable.Compression {
		// compression turned off
		return nil, nil
	}
	if requestKey == CompressionDictKey {
		// if it is requested for compression dict read itself
		return compression.NewDictionary([]byte(CompressionDictKey)), nil
	}
	bytes, err := kvs.get(CompressionDictKey)
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		// first ever read of compression dictionary
		return compression.NewDictionary([]byte(CompressionDictKey)), nil
	}
	return compression.Deserialize(bytes), nil
}

// updateCompressionDict adds the given key to the global compression dictionary and returns the updated dictionary.
// If the compression is turned off, does nothing and returns nil.
// If the value of key is CompressionDictKey no additional operation is performed on the engine
// and a new compression dictionary containing only CompressionDictKey mapping is returned.
// This ensures that at most one call to kvs.get() is made
// and that the dictionary is saved to the database iff there is at least one key that is not CompressionDictKey.
// Makes an additional call to kvs.put() only if a new key was actually added.
func (kvs *KeyValueStore) updateCompressionDict(key string) (*compression.Dictionary, error) {
	if !kvs.config.SSTable.Compression {
		// compression turned off
		return nil, nil
	}
	if key == CompressionDictKey {
		// if it is requested for compression dict put itself
		return compression.NewDictionary([]byte(CompressionDictKey)), nil
	}
	bytes, err := kvs.get(CompressionDictKey)
	if err != nil {
		return nil, err
	}

	var dict *compression.Dictionary
	if bytes == nil {
		// first ever read of compression dictionary
		dict = compression.NewDictionary([]byte(CompressionDictKey))
	} else {
		dict, err = compression.Deserialize(bytes), nil
		if err != nil {
			return nil, err
		}
	}

	added := dict.Add([]byte(key))

	if added {
		// run put only if dictionary has actually changed
		err = kvs.put(CompressionDictKey, dict.Serialize())
		if err != nil {
			return nil, err
		}
	}

	return dict, nil
}
