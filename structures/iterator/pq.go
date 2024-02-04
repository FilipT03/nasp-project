package iterator

import (
	"bytes"
	"nasp-project/util"
)

// PriorityQueue is a priority queue of iterators that implements heap.Interface.
type PriorityQueue []util.Iterator

func (h PriorityQueue) Len() int { return len(h) }

func (h PriorityQueue) Less(i, j int) bool {
	if bytes.Equal(h[i].Value().Key, h[j].Value().Key) {
		return h[i].Value().Timestamp > h[j].Value().Timestamp
	}
	return bytes.Compare(h[i].Value().Key, h[j].Value().Key) < 0
}

func (h PriorityQueue) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *PriorityQueue) Push(x any) {
	*h = append(*h, x.(util.Iterator))
}

func (h *PriorityQueue) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
