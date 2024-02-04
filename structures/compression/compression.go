package compression

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

// Dictionary contains two-way mapping between db keys and integers.
// The key of the dictionary record itself should always be mapped to 0.
type Dictionary struct {
	keys   [][]byte       // array of keys added to dictionary, maps index i to i-th element in array (0 based)
	idxMap map[string]int // maps key to its index in keys
}

// NewDictionary creates a new compression dictionary.
func NewDictionary() *Dictionary {
	return &Dictionary{
		keys:   [][]byte{},
		idxMap: map[string]int{},
	}
}

// Add the key to the dictionary if it is not present, does nothing otherwise.
// Returns true if a new key is added.
func (d *Dictionary) Add(key []byte) bool {
	_, ok := d.idxMap[string(key)]
	if ok {
		return false
	}
	d.keys = append(d.keys, key)
	d.idxMap[string(key)] = len(d.keys) - 1
	return true
}

// GetIdx returns the index of the given key if it exists in the dictionary, -1 otherwise.
func (d *Dictionary) GetIdx(key []byte) int {
	if idx, ok := d.idxMap[string(key)]; ok {
		return idx
	}
	return -1
}

// GetKey returns the key at the given index if it exists, nil otherwise.
func (d *Dictionary) GetKey(idx int) []byte {
	if idx >= 0 && idx < len(d.keys) {
		return d.keys[idx]
	}
	return nil
}

func (d *Dictionary) Serialize() []byte {
	ret := make([]byte, 0)
	for _, key := range d.keys {
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, uint64(len(key)))
		ret = append(ret, buf[:n]...)
		ret = append(ret, key...)
	}
	return ret
}

func Deserialize(data []byte) *Dictionary {
	var keys [][]byte
	offset := 0
	for {
		keySize, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			break
		}
		key := data[offset+n : offset+n+int(keySize)]
		keys = append(keys, key)
		offset += n + int(keySize)
	}
	dict := Dictionary{
		keys:   keys,
		idxMap: map[string]int{},
	}
	for idx, key := range keys {
		dict.idxMap[string(key)] = idx
	}
	return &dict
}

func LoadCompressionDictFromFile(savePath, filename string) (*Dictionary, error) {
	path := filepath.Join(savePath, filename)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return NewDictionary(), nil
	} else if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Deserialize(data), nil
}

func WriteCompressionDictToFile(compressionDict *Dictionary, savePath, filename string) error {
	file, err := os.Create(filepath.Join(savePath, filename))
	if os.IsNotExist(err) {
		err := os.MkdirAll(savePath, 0755)
		if err != nil {
			return err
		}
		file, err = os.Create(filepath.Join(savePath, filename))
	} else if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(compressionDict.Serialize())
	if err != nil {
		return err
	}
	return nil
}

// AppendLastToFile assumes that all but last record are written to file and appends the last record to the end.
// Call this function after Dictionary.Add returns true to update the structure on disk.
func (d *Dictionary) AppendLastToFile(savePath, filename string) error {
	file, err := os.OpenFile(filepath.Join(savePath, filename), os.O_APPEND|os.O_WRONLY, 0644)
	if os.IsNotExist(err) {
		err := os.MkdirAll(savePath, 0755)
		if err != nil {
			return err
		}
		file, err = os.Create(filepath.Join(savePath, filename))
	} else if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(len(d.keys[len(d.keys)-1])))
	_, err = file.Write(buf[:n])
	if err != nil {
		return err
	}

	_, err = file.Write(d.keys[len(d.keys)-1])
	if err != nil {
		return err
	}

	return nil
}
