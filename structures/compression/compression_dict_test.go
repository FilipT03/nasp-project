package compression

import "testing"

const CompressionDictKey = "COMP_DICT"

func TestNewDictionary(t *testing.T) {
	dict := NewDictionary([]byte(CompressionDictKey))
	if dict == nil {
		t.Fatalf("Failed to create a new compression dictionary.")
	}
	if dict.keys == nil {
		t.Fatalf("Expected keys to be initialized.")
	}
	if dict.idxMap == nil {
		t.Fatalf("Expected idxMap to be initialized.")
	}
	if len(dict.keys) != 1 {
		t.Errorf("Expected 1 key, got %d.", len(dict.keys))
	}
	if len(dict.idxMap) != 1 {
		t.Errorf("Expected 1 key, got %d.", len(dict.idxMap))
	}
	if string(dict.keys[0]) != CompressionDictKey {
		t.Errorf("Expected %s, got %s in keys array.", CompressionDictKey, string(dict.keys[0]))
	}
	if dict.idxMap[string(dict.keys[0])] != 0 {
		t.Errorf("Expected 0, got %d in idxMap.", dict.idxMap[string(dict.keys[0])])
	}
}

func TestDictionary_Add(t *testing.T) {
	dict := NewDictionary([]byte(CompressionDictKey))
	if dict == nil {
		t.Fatalf("Failed to create a new compression dictionary.")
	}

	key := []byte("key")
	dict.Add(key)
	if len(dict.keys) != 2 {
		t.Errorf("Expected 2 keys, got %d.", len(dict.keys))
	}
	if len(dict.idxMap) != 2 {
		t.Errorf("Expected 2 keys, got %d.", len(dict.idxMap))
	}
	if string(dict.keys[1]) != string(key) {
		t.Errorf("Expected %s, got %s in keys array.", string(key), string(dict.keys[1]))
	}
	if dict.idxMap[string(dict.keys[1])] != 1 {
		t.Errorf("Expected 1, got %d in idxMap.", dict.idxMap[string(dict.keys[1])])
	}

	dict.Add(key)
	if len(dict.keys) != 2 {
		t.Errorf("Expected 2 keys, got %d.", len(dict.keys))
	}
	if len(dict.idxMap) != 2 {
		t.Errorf("Expected 2 keys, got %d.", len(dict.idxMap))
	}
	if string(dict.keys[1]) != string(key) {
		t.Errorf("Expected %s, got %s in keys array.", string(key), string(dict.keys[1]))
	}
	if dict.idxMap[string(dict.keys[1])] != 1 {
		t.Errorf("Expected 1, got %d in idxMap.", dict.idxMap[string(dict.keys[1])])
	}
}

func TestDictionary_GetIdx(t *testing.T) {
	dict := NewDictionary([]byte(CompressionDictKey))
	if dict == nil {
		t.Fatalf("Failed to create a new compression dictionary.")
	}

	key := []byte("key")
	dict.Add(key)
	idx := dict.GetIdx(key)
	if idx != 1 {
		t.Errorf("Expected 1, got %d.", idx)
	}

	idx = dict.GetIdx([]byte("non-existent"))
	if idx != -1 {
		t.Errorf("Expected -1, got %d.", idx)
	}
}

func TestDictionary_GetKey(t *testing.T) {
	dict := NewDictionary([]byte(CompressionDictKey))
	if dict == nil {
		t.Fatalf("Failed to create a new compression dictionary.")
	}

	key := []byte("key")
	dict.Add(key)
	got := dict.GetKey(1)
	if string(got) != string(key) {
		t.Errorf("Expected %s, got %s.", string(key), string(got))
	}

	got = dict.GetKey(2)
	if got != nil {
		t.Errorf("Expected nil, got %s.", string(got))
	}
}

func TestSerialize(t *testing.T) {
	dict := NewDictionary([]byte(CompressionDictKey))
	if dict == nil {
		t.Fatalf("Failed to create a new compression dictionary.")
	}

	key := []byte("key")
	dict.Add(key)
	serialized := dict.Serialize()
	deserialized := Deserialize(serialized)
	if len(deserialized.keys) != 2 {
		t.Errorf("Expected 2 keys, got %d.", len(deserialized.keys))
	}
	if len(deserialized.idxMap) != 2 {
		t.Errorf("Expected 2 keys, got %d.", len(deserialized.idxMap))
	}
	if string(deserialized.keys[1]) != string(key) {
		t.Errorf("Expected %s, got %s in keys array.", string(key), string(deserialized.keys[1]))
	}
	if deserialized.idxMap[string(deserialized.keys[1])] != 1 {
		t.Errorf("Expected 1, got %d in idxMap.", deserialized.idxMap[string(deserialized.keys[1])])
	}
}
