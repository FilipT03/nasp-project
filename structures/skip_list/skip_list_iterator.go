package skip_list

import (
	"errors"
	"nasp-project/model"
	"nasp-project/util"
)

type SkipListIter struct {
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
	return &SkipListIter{current: starterNode.next}, nil
}

func (iter *SkipListIter) Next() bool {
	if iter.current.next != nil {
		iter.current = iter.current.next
		return true
	}
	iter.current = nil
	return false
}

func (iter *SkipListIter) Value() *model.Record {
	if iter.current != nil {
		return iter.current.record
	}
	return nil
}
