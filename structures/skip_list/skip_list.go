package skip_list

import (
	"errors"
	"fmt"
	"math/rand"
	"nasp-project/structures/item"
	"strings"
)

type skipListNode struct {
	next, down *skipListNode
	item       *item.Item
}

type SkipList struct {
	head      *skipListNode // first node
	m         uint32        // maximal number of nodes
	size      uint32        // number of nodes currently in the skip list
	height    uint32        // height of the tallest column including the first
	isLimited bool
}

// NewSkipList creates a new empty skip list of the specified size.
func NewSkipListOfSize(m uint32) *SkipList {
	head := skipListNode{}
	return &SkipList{
		&head,
		m,
		0,
		1,
		true,
	}
}

// NewSkipList creates a new empty skip list of unlimited size.
func NewSkipList() *SkipList {
	head := skipListNode{}
	return &SkipList{
		&head,
		0,
		0,
		1,
		false,
	}
}

// HasKey checks if the specified key is in the skip list.
func (sl *SkipList) HasKey(key string) bool {
	resultNode := sl.searchForKey(key)

	if resultNode.item == nil || resultNode.item.Key != key {
		return false
	} else {
		return true
	}
}

// Add attempts to add a new item to the skip list. Returns error if the key is already present or the list is full.
// New node points to the item sent as an argument.
func (sl *SkipList) Add(item *item.Item) error {
	/*   x     o
		 x  x  o
		 o ox oo
	     oooxxoo
		 -------
		 124568nil
	*/ //Let's say this is our skip list. If we were searching for the 7, the search would follow the path marked by the
	//   x's. The rightmost x's in each row are the ones who may point to our new element, depending on the height of
	//   the new column. We will do the search twice, redirecting nodes on the second go.

	resultNode := sl.searchForKey(item.Key)
	if resultNode.item != nil && resultNode.item.Key == item.Key {
		return errors.New("the key is already present in the skip list")
	}
	if sl.isLimited && sl.size == sl.m {
		return errors.New("the skip list is already full")
	}
	sl.size++

	var newHeight uint32 = 1
	for rand.Intn(2) == 1 {
		newHeight++
	}

	for sl.height <= newHeight {
		sl.head = &skipListNode{nil, sl.head, nil}
		sl.height++
	}
	currentHeight := sl.height

	currentNode := sl.head
	var lastAddedNode *skipListNode = nil // keeping track of the last added node se we can redirect its down pointer.
	for {
		if currentNode.next == nil || item.Key < currentNode.next.item.Key {
			if currentHeight <= newHeight { // we are adding a new node
				currentNode.next = &skipListNode{currentNode.next, nil, item}
				if lastAddedNode != nil {
					lastAddedNode.down = currentNode.next
				}
				lastAddedNode = currentNode.next
			}
			if currentNode.down == nil { // we reached the bottom
				break
			}
			currentNode = currentNode.down
			currentHeight--
		} else {
			if currentNode.next == nil { // we reached the rightmost column
				break
			}
			currentNode = currentNode.next
		}
	}
	return nil
}

// Searches for the key and returns the end node from which state can be determined.
func (sl *SkipList) searchForKey(key string) *skipListNode {
	currentNode := sl.head
	for {
		if currentNode.next == nil || key < currentNode.next.item.Key {
			if currentNode.down == nil { // we reached the bottom
				break
			}
			currentNode = currentNode.down
		} else {
			if currentNode.next == nil { // we reached the rightmost column
				break
			}
			currentNode = currentNode.next
		}
	}
	return currentNode
}

// Print the skip list
func (sl *SkipList) Print() {
	starterNode := sl.head
	for starterNode != nil {
		currentNode := starterNode
		for currentNode != nil {
			if currentNode.item == nil {
				fmt.Print("nil->")
			} else {
				fmt.Print(currentNode.item.Key + "->")
			}
			if currentNode.down != nil { // find how far the next node is to print spaces
				lowestNode := currentNode.down // we need the lowest layer because it's always filled
				for lowestNode.down != nil {
					lowestNode = lowestNode.down
				}
				if currentNode.next == nil || currentNode.next.item == nil {
					for lowestNode.next != nil {
						lowestNode = lowestNode.next
						print(strings.Repeat(" ", 2+len(lowestNode.item.Key)))
					}
				} else {
					for lowestNode.next.item != currentNode.next.item {
						lowestNode = lowestNode.next
						print(strings.Repeat(" ", 2+len(lowestNode.item.Key)))
					}
				}
			}
			currentNode = currentNode.next
		}
		println("nil")
		starterNode = starterNode.down
	}
}
