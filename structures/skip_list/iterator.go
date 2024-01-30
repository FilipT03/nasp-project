package skip_list

import (
	"errors"
	"nasp-project/model"
	"nasp-project/util"
)

type Iterator struct {
	current *skipListNode
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
