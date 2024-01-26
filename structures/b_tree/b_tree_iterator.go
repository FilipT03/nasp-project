package b_tree

import (
	"errors"
	"nasp-project/model"
	"nasp-project/util"
)

type BTreeIter struct {
	records  []*model.Record
	index    int
	maxIndex int
}

func (b *BTreeIter) Next() bool {
	b.index += 1
	return b.index < b.maxIndex
}

func (b *BTreeIter) Value() *model.Record {
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
	return &BTreeIter{
		records:  records,
		index:    0,
		maxIndex: len(records),
	}, nil
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
