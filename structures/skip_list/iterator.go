package skip_list

import (
	"bytes"
	"errors"
	"nasp-project/model"
	"nasp-project/util"
)

type Iterator struct {
	current *skipListNode
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

func (sl *SkipList) NewIterator() (util.Iterator, error) {
	if sl.size == 0 {
		return nil, errors.New("error: SkipList is empty")
	}
	starterNode := sl.head
	height := sl.height

	for height != 1 {
		starterNode = starterNode.down
		height--
	}

	iter := &Iterator{current: starterNode.next}
	for util.IsInvalidKey(iter) {
		starterNode = starterNode.next
		iter.current = starterNode
	}
	return iter, nil
}

func (iter *Iterator) Next() bool {
	for iter.current.next != nil {
		iter.current = iter.current.next
		if !util.IsInvalidKey(iter) {
			return true
		}
	}
	iter.current = nil
	return false
}

func (iter *Iterator) Value() *model.Record {
	if iter.current != nil {
		return iter.current.record
	}
	return nil
}

func (sl *SkipList) NewRangeIterator(startKey []byte, endKey []byte) (util.Iterator, error) {
	iter, err := sl.NewIterator()
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
		iter.Iterator.current = nil
		return false
	}
	return true
}

func (iter *RangeIterator) Value() *model.Record {
	return iter.Iterator.Value()
}

func (sl *SkipList) NewPrefixIterator(prefix []byte) (util.Iterator, error) {
	iter, err := sl.NewIterator()
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
		iter.Iterator.current = nil
		return false
	}
	return true
}

func (iter *PrefixIterator) Value() *model.Record {
	return iter.Iterator.Value()
}
