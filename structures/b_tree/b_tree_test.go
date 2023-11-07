package b_tree

import (
	"testing"
)

func TestGet(t *testing.T) {
	bt := NewBTree(2)
	bt.Add("1", []byte("1"))
	bt.Add("2", []byte("2"))
	bt.Add("3", []byte("3"))
	bt.Add("4", []byte("4"))
	bt.Add("5", []byte("5"))
	bt.Add("6", []byte("6"))
	bt.Add("7", []byte("7"))
	bt.Add("8", []byte("8"))

	val, err := bt.Get("2")
	if err != nil {
		t.Error(err)
	}
	if string(val) != "2" {
		t.Errorf("Expected \"2\" got %v", val)
	}
	val, err = bt.Get("4")
	if err != nil {
		t.Error(err)
	}
	if string(val) != "4" {
		t.Errorf("Expected \"4\" got %v", val)
	}
	val, err = bt.Get("8")
	if err != nil {
		t.Error(err)
	}
	if string(val) != "8" {
		t.Errorf("Expected \"8\" got %v", val)
	}
}

func TestSplit(t *testing.T) {
	bt := NewBTree(3)

	bt.Add("2", []byte{1})
	bt.Add("5", []byte{1})
	bt.Add("9", []byte{1})
	bt.Add("7", []byte{1})
	bt.Add("3", []byte{1})
	bt.Add("8", []byte{1})
	bt.Add("A", []byte{1})
	bt.Add("D", []byte{1})
	bt.Add("B", []byte{1})
	bt.Add("E", []byte{1})
	bt.Add("F", []byte{1})

	rootNode := bt.root
	if len(rootNode.items) != 2 {
		t.Errorf("Expected 2 items in the root node, but got %d", len(rootNode.items))
	}
	if len(bt.root.children) != 3 {
		t.Errorf("Expected 3 children in the root node, but got %d\"", len(rootNode.children))
	}
	if bt.root.items[0].key != "7" {
		t.Errorf("Expected first key in root node to be 7, but got %s", rootNode.items[0].key)
	}
	if bt.root.items[1].key != "B" {
		t.Errorf("Expected second key in root node to be B, but got %s", rootNode.items[1].key)
	}

	leftNode := bt.root.children[0]
	if len(leftNode.items) != 3 {
		t.Errorf("Expected 3 items in the left node, but got %d", len(bt.root.items))
	}
	for i := 0; i < len(leftNode.items); i++ {
		if leftNode.items[i].key > rootNode.items[0].key {
			t.Errorf("Key %s in the left node at index %d is greater than the key in the root node at index 0", leftNode.items[i].key, i)
		}
	}

	middleNode := bt.root.children[1]
	if len(middleNode.items) != 3 {
		t.Errorf("Expected 3 items in the left node, but got %d\"", len(bt.root.items))
	}
	for i := 0; i < len(middleNode.items); i++ {
		if middleNode.items[i].key > rootNode.items[1].key {
			t.Errorf("Key %s in the middle node at index %d is greater than the key in the root node at index 0", middleNode.items[i].key, i)
		}
		if middleNode.items[i].key < rootNode.items[0].key {
			t.Errorf("Key %s in the middle node at index %d is less than the key in the root node at index 0", middleNode.items[i].key, i)
		}
	}

	rightNode := bt.root.children[2]
	if len(rightNode.items) != 3 {
		t.Errorf("Expected 3 items in the right node, but got %d\"", len(bt.root.items))
	}
	for i := 0; i < len(rightNode.items); i++ {
		if rightNode.items[i].key < rootNode.items[1].key {
			t.Errorf("Key %s in right node at index %d is less than the key in the root node at index 1", rightNode.items[i].key, i)
		}
	}
}

func TestLeftRotation(t *testing.T) {
	bt := NewBTree(3)

	bt.Add("2", []byte{1})
	bt.Add("4", []byte{1})
	bt.Add("8", []byte{1})
	bt.Add("9", []byte{1})
	bt.Add("A", []byte{1})
	bt.Add("C", []byte{1})
	bt.Add("E", []byte{1})
	bt.Add("H", []byte{1})

	err := bt.Delete("8")
	if err != nil {
		t.Error(err)
	}

	if _, err := bt.Get("8"); err == nil {
		t.Error("Found deleted key")
	}

	rootNode := bt.root
	leftNode := rootNode.children[0]
	for i := 0; i < len(leftNode.items); i++ {
		if leftNode.items[i].key > rootNode.items[0].key {
			t.Errorf("Key %s in the left node at index %d is greater than the key in the root node at index 0", leftNode.items[i].key, i)
		}
	}

	rightNode := rootNode.children[1]
	for i := 0; i < len(rightNode.items); i++ {
		if rightNode.items[i].key < rootNode.items[0].key {
			t.Errorf("Key %s in right node at index %d is less than the key in the root node at index 1", rightNode.items[i].key, i)
		}
	}
}

func TestMergeHeightLoss(t *testing.T) {
	bt := NewBTree(3)

	bt.Add("2", []byte{1})
	bt.Add("9", []byte{1})
	bt.Add("4", []byte{1})
	bt.Add("8", []byte{1})
	bt.Add("A", []byte{1})
	bt.Add("C", []byte{1})
	bt.Add("G", []byte{1})

	err := bt.Delete("C")
	if err != nil {
		t.Error(err)
	}
	if _, err := bt.Get("C"); err == nil {
		t.Error("Found deleted key")
	}
	if len(bt.root.items) != 6 {
		t.Errorf("Expected 6 items in the root node, but got %d\"", len(bt.root.items))
	}
}
