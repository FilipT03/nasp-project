package b_tree

import (
	"bytes"
	"nasp-project/model"
	"testing"
)

func TestFlush(t *testing.T) {
	bt := NewBTree(3)

	_ = bt.Add(&model.Record{
		Key:       []byte("1"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Key:       []byte("5"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Key:       []byte("7"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	_ = bt.Add(&model.Record{
		Key:       []byte("8"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Key:       []byte("4"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Key:       []byte("2"),
		Value:     nil,
		Tombstone: false,
		Timestamp: 0,
	})

	records := bt.Flush()
	sol := [][]byte{[]byte("1"), []byte("2"), []byte("4"), []byte("5"), []byte("7"), []byte("8")}
	for i, record := range records {
		if bytes.Compare(record.Key, sol[i]) != 0 {
			t.Errorf("error: keys are not sorted correcly at %d", i)
		}
	}
}

func TestSize(t *testing.T) {
	bt := NewBTree(2)

	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("1"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("1"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("2"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("22"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("1"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("6"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("9"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("69"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("23"),
		Value:     nil,
		Timestamp: 0,
	})

	if bt.size != 7 {
		t.Errorf("Expected size to be 7, got %d", bt.size)
	}
}

func TestGet(t *testing.T) {
	bt := NewBTree(2)

	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("1"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("2"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("3"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("4"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("5"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("6"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("7"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("8"),
		Value:     nil,
		Timestamp: 0,
	})

	val, err := bt.Get([]byte("2"))
	if err != nil {
		t.Error(err)
	}
	if string(val.Key) != "2" {
		t.Errorf("Expected \"2\" got %v", val)
	}
	val, err = bt.Get([]byte("4"))
	if err != nil {
		t.Error(err)
	}
	if string(val.Key) != "4" {
		t.Errorf("Expected \"4\" got %v", val)
	}
	val, err = bt.Get([]byte("8"))
	if err != nil {
		t.Error(err)
	}
	if string(val.Key) != "8" {
		t.Errorf("Expected \"8\" got %v", val)
	}
}

func TestSplit(t *testing.T) {
	bt := NewBTree(3)

	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("2"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("5"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("9"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("7"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("3"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("8"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("A"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("D"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("B"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("E"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("F"),
		Value:     nil,
		Timestamp: 0,
	})

	rootNode := bt.root
	if len(rootNode.records) != 2 {
		t.Errorf("Expected 2 records in the root node, but got %d", len(rootNode.records))
	}
	if len(bt.root.children) != 3 {
		t.Errorf("Expected 3 children in the root node, but got %d\"", len(rootNode.children))
	}
	if string(bt.root.records[0].Key) != "7" {
		t.Errorf("Expected first key in root node to be 7, but got %s", string(rootNode.records[0].Key))
	}
	if string(bt.root.records[1].Key) != "B" {
		t.Errorf("Expected second key in root node to be B, but got %s", string(rootNode.records[1].Key))
	}

	leftNode := bt.root.children[0]
	if len(leftNode.records) != 3 {
		t.Errorf("Expected 3 records in the left node, but got %d", len(bt.root.records))
	}
	for i := 0; i < len(leftNode.records); i++ {
		if string(leftNode.records[i].Key) > string(rootNode.records[0].Key) {
			t.Errorf("Key %s in the left node at index %d is greater than the key in the root node at index 0", string(leftNode.records[i].Key), i)
		}
	}

	middleNode := bt.root.children[1]
	if len(middleNode.records) != 3 {
		t.Errorf("Expected 3 records in the left node, but got %d\"", len(bt.root.records))
	}
	for i := 0; i < len(middleNode.records); i++ {
		if string(middleNode.records[i].Key) > string(rootNode.records[1].Key) {
			t.Errorf("Key %s in the middle node at index %d is greater than the key in the root node at index 0", string(middleNode.records[i].Key), i)
		}
		if string(middleNode.records[i].Key) < string(rootNode.records[0].Key) {
			t.Errorf("Key %s in the middle node at index %d is less than the key in the root node at index 0", string(middleNode.records[i].Key), i)
		}
	}

	rightNode := bt.root.children[2]
	if len(rightNode.records) != 3 {
		t.Errorf("Expected 3 records in the right node, but got %d\"", len(bt.root.records))
	}
	for i := 0; i < len(rightNode.records); i++ {
		if string(rightNode.records[i].Key) < string(rootNode.records[1].Key) {
			t.Errorf("Key %s in right node at index %d is less than the key in the root node at index 1", string(rightNode.records[i].Key), i)
		}
	}
}

func TestLeftRotation(t *testing.T) {
	bt := NewBTree(3)

	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("2"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("4"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("8"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("9"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("A"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("C"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("E"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("H"),
		Value:     nil,
		Timestamp: 0,
	})

	err := bt.Remove([]byte("8"))
	if err != nil {
		t.Error(err)
	}

	if _, err := bt.Get([]byte("8")); err == nil {
		t.Error("Found deleted key")
	}

	rootNode := bt.root
	leftNode := rootNode.children[0]
	for i := 0; i < len(leftNode.records); i++ {
		if string(leftNode.records[i].Key) > string(rootNode.records[0].Key) {
			t.Errorf("Key %s in the left node at index %d is greater than the key in the root node at index 0", string(leftNode.records[i].Key), i)
		}
	}

	rightNode := rootNode.children[1]
	for i := 0; i < len(rightNode.records); i++ {
		if string(rightNode.records[i].Key) < string(rootNode.records[0].Key) {
			t.Errorf("Key %s in right node at index %d is less than the key in the root node at index 1", string(rightNode.records[i].Key), i)
		}
	}
}

func TestMergeHeightLoss(t *testing.T) {
	bt := NewBTree(3)

	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("2"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("9"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("4"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("8"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("A"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("C"),
		Value:     nil,
		Timestamp: 0,
	})
	_ = bt.Add(&model.Record{
		Tombstone: false,
		Key:       []byte("G"),
		Value:     nil,
		Timestamp: 0,
	})

	err := bt.Remove([]byte("C"))
	if err != nil {
		t.Error(err)
	}
	if record, err := bt.Get([]byte("C")); err == nil {
		if !record.Tombstone {
			t.Error("Found deleted key")
		}
	}
	if len(bt.root.records) != 6 {
		t.Errorf("Expected 6 records in the root node, but got %d\"", len(bt.root.records))
	}
}
