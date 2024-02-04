package b_tree

import (
	"bytes"
	"errors"
	"nasp-project/model"
	"nasp-project/util"
)

type Iterator struct {
	records  []*model.Record
	index    int
	maxIndex int
}

type RangeIterator struct {
	Iterator
	startKey []byte
	endKey   []byte
}

type PrefixIterator struct {
	Iterator
	prefix []byte
}

func (b *Iterator) Next() bool {
	b.index += 1
	for util.IsInvalidKey(b) {
		b.index += 1
	}
	return b.index < b.maxIndex
}

func (b *Iterator) Value() *model.Record {
	if b.index < b.maxIndex {
		return b.records[b.index]
	}
	return nil
}

func (bt *BTree) NewIterator() (util.Iterator, error) {
	if bt.size == 0 {
		return nil, errors.New("error: btree is empty")
	}
	records := getRecords(bt.root)

	iter := &Iterator{
		records:  records,
		index:    0,
		maxIndex: len(records),
	}
	for util.IsInvalidKey(iter) {
		iter.index += 1
	}
	return iter, nil
}

func getRecords(node *Node) (slice []*model.Record) {
	if node.isLeaf() {
		for _, record := range node.records {
			slice = append(slice, record)
		}
		return
	}
	for i, child := range node.children {
		slice = append(slice, getRecords(child)...)
		if i < len(node.children)-1 {
			slice = append(slice, node.records[i])
		}
	}
	return
}

func (bt *BTree) NewRangeIterator(startKey []byte, endKey []byte) (util.Iterator, error) {
	iter, err := bt.NewIterator()
	if err != nil {
		return nil, err
	}

	for bytes.Compare(iter.Value().Key, startKey) < 0 {
		if !iter.Next() {
			return nil, errors.New("error: could not find startKey")
		}
	}

	return &RangeIterator{
		Iterator: *iter.(*Iterator),
		startKey: startKey,
		endKey:   endKey,
	}, nil
}

func (iter *RangeIterator) Next() bool {
	if !iter.Iterator.Next() {
		return false
	}
	if bytes.Compare(iter.Value().Key, iter.endKey) > 0 {
		return false
	}
	return true
}

func (iter *RangeIterator) Value() *model.Record {
	return iter.Iterator.Value()
}

func (bt *BTree) NewPrefixIterator(prefix []byte) (util.Iterator, error) {
	iter, err := bt.NewIterator()
	if err != nil {
		return nil, err
	}

	for !bytes.HasPrefix(iter.Value().Key, prefix) {
		if !iter.Next() {
			return nil, errors.New("error: could not find prefix")
		}
	}

	return &PrefixIterator{
		Iterator: *iter.(*Iterator),
		prefix:   prefix,
	}, nil
}

func (iter *PrefixIterator) Next() bool {
	if !iter.Iterator.Next() {
		return false
	}
	for !bytes.HasPrefix(iter.Value().Key, iter.prefix) {
		return false
	}
	return true
}

func (iter *PrefixIterator) Value() *model.Record {
	return iter.Iterator.Value()
}
