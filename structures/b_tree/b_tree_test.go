package b_tree

import (
	"testing"
)

func TestGet(t *testing.T) {
	bt := NewBTree(2)
	_ = bt.Add("1", []byte("1"))
	_ = bt.Add("2", []byte("2"))
	_ = bt.Add("3", []byte("3"))
	_ = bt.Add("4", []byte("4"))
	_ = bt.Add("5", []byte("5"))
	_ = bt.Add("6", []byte("6"))
	_ = bt.Add("7", []byte("7"))
	_ = bt.Add("8", []byte("8"))

	val, err := bt.Get("2")
	if err != nil {
		t.Error(err)
	}
	if string(val.Value) != "2" {
		t.Errorf("Expected \"2\" got %v", val)
	}
	val, err = bt.Get("4")
	if err != nil {
		t.Error(err)
	}
	if string(val.Value) != "4" {
		t.Errorf("Expected \"4\" got %v", val)
	}
	val, err = bt.Get("8")
	if err != nil {
		t.Error(err)
	}
	if string(val.Value) != "8" {
		t.Errorf("Expected \"8\" got %v", val)
	}
}

func TestSplit(t *testing.T) {
	bt := NewBTree(3)

	_ = bt.Add("2", []byte{1})
	_ = bt.Add("5", []byte{1})
	_ = bt.Add("9", []byte{1})
	_ = bt.Add("7", []byte{1})
	_ = bt.Add("3", []byte{1})
	_ = bt.Add("8", []byte{1})
	_ = bt.Add("A", []byte{1})
	_ = bt.Add("D", []byte{1})
	_ = bt.Add("B", []byte{1})
	_ = bt.Add("E", []byte{1})
	_ = bt.Add("F", []byte{1})

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

	_ = bt.Add("2", []byte{1})
	_ = bt.Add("4", []byte{1})
	_ = bt.Add("8", []byte{1})
	_ = bt.Add("9", []byte{1})
	_ = bt.Add("A", []byte{1})
	_ = bt.Add("C", []byte{1})
	_ = bt.Add("E", []byte{1})
	_ = bt.Add("H", []byte{1})

	err := bt.Delete("8")
	if err != nil {
		t.Error(err)
	}

	if _, err := bt.Get("8"); err == nil {
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

	_ = bt.Add("2", []byte{1})
	_ = bt.Add("9", []byte{1})
	_ = bt.Add("4", []byte{1})
	_ = bt.Add("8", []byte{1})
	_ = bt.Add("A", []byte{1})
	_ = bt.Add("C", []byte{1})
	_ = bt.Add("G", []byte{1})

	err := bt.Delete("C")
	if err != nil {
		t.Error(err)
	}
	if _, err := bt.Get("C"); err == nil {
		t.Error("Found deleted key")
	}
	if len(bt.root.records) != 6 {
		t.Errorf("Expected 6 records in the root node, but got %d\"", len(bt.root.records))
	}
}
