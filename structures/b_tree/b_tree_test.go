package b_tree

import "testing"

func TestAdd(t *testing.T) {
	bt := NewBTree(2)
	bt.Add("1", []byte("1"))
	bt.Add("2", []byte("2"))
	bt.Add("3", []byte("3"))
	bt.Add("4", []byte("4"))
	bt.Add("5", []byte("5"))
	bt.Add("6", []byte("6"))
	bt.Add("7", []byte("7"))
	bt.Add("8", []byte("8"))

	val, err := bt.Find("2")
	if err != nil {
		t.Error(err)
	}
	if string(val) != "2" {
		t.Errorf("Expected \"2\" got %v", val)
	}
	val, err = bt.Find("4")
	if err != nil {
		t.Error(err)
	}
	if string(val) != "4" {
		t.Errorf("Expected \"4\" got %v", val)
	}
	val, err = bt.Find("8")
	if err != nil {
		t.Error(err)
	}
	if string(val) != "8" {
		t.Errorf("Expected \"8\" got %v", val)
	}
}
