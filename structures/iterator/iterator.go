package iterator

import (
	"bytes"
	"container/heap"
	"nasp-project/model"
	"nasp-project/util"
)

// Iterator combines multiple key-sorted iterators into a single key-sorted iterator.
type Iterator struct {
	pq PriorityQueue
}

// NewIterator creates a new iterator that iterates through the given iterators.
func NewIterator(iterators []util.Iterator) (*Iterator, error) {
	pq := make(PriorityQueue, 0, len(iterators))
	for _, it := range iterators {
		if it.Value() != nil {
			pq = append(pq, it)
		}
	}
	heap.Init(&pq)
	return &Iterator{pq}, nil
}

// Value returns the current record of the iterator.
func (it *Iterator) Value() *model.Record {
	if len(it.pq) == 0 {
		return nil
	}
	return it.pq[0].Value()
}

// Next returns the current record of the iterator and moves the iterator to the next record.
// Skips reserved keys. The key is reserved is util.IsReservedKey returns true.
func (it *Iterator) Next() *model.Record {
	var rec, prevRec, currRec *model.Record
	for len(it.pq) > 0 {
		iter := it.pq[0]
		currRec = iter.Value()

		if rec == nil {
			rec = currRec
		}

		if prevRec != nil && !bytes.Equal(currRec.Key, prevRec.Key) {
			// call next for all records with the same key
			break
		}

		if iter.Next() {
			heap.Fix(&it.pq, 0)
		} else {
			heap.Pop(&it.pq)
		}

		prevRec = currRec
	}
	return rec
}

// Stop stops end invalidates the iterator.
func (it *Iterator) Stop() {
	it.pq = nil
}
