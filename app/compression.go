package app

import (
	"nasp-project/structures/compression"
)

// getCompressionDict returns the current global compression dictionary.
// If the compression is turned off, returns nil.
func (kvs *KeyValueStore) getCompressionDict() (*compression.Dictionary, error) {
	if !kvs.config.SSTable.Compression {
		// compression turned off
		return nil, nil
	}
	return compression.LoadCompressionDictFromFile(kvs.config.SSTable.SavePath, kvs.config.SSTable.CompressionFilename)
}

// updateCompressionDict adds the given key to the global compression dictionary and returns the updated dictionary.
// If the compression is turned off, does nothing and returns nil.
func (kvs *KeyValueStore) updateCompressionDict(key string) (*compression.Dictionary, error) {
	if !kvs.config.SSTable.Compression {
		// compression turned off
		return nil, nil
	}

	compressionDict, err := kvs.getCompressionDict()
	if err != nil {
		return nil, err
	}

	added := compressionDict.Add([]byte(key))

	if added {
		err = compression.WriteCompressionDictToFile(compressionDict, kvs.config.SSTable.SavePath, kvs.config.SSTable.CompressionFilename)
		if err != nil {
			return nil, err
		}
	}

	return compressionDict, nil
}
