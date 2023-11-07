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
	maxSize   uint32        // maximal number of nodes
	size      uint32        // number of nodes currently in the skip list
	maxHeight uint32        // maximal height, recommended to be at least log2(maxSize)
	height    uint32        // height of the tallest column including the first
}

// NewSkipList creates a new empty skip list of the specified size.
func NewSkipList(maxSize uint32, maxHeight uint32) *SkipList {
	head := skipListNode{}
	return &SkipList{
		&head,
		maxSize,
		0,
		maxHeight,
		1,
	}
}

// HasKey checks if the specified key is in the skip list.
func (sl *SkipList) HasKey(key string) bool {
	resultNode := sl.searchForKey(key)
	return resultNode.item != nil && resultNode.item.Key == key
}

// Get returns the value of the item with the specified key if present. If not, returns an error.
func (sl *SkipList) Get(key string) ([]byte, error) {
	resultNode := sl.searchForKey(key)
	if resultNode.item != nil && resultNode.item.Key == key {
		return resultNode.item.Value, nil
	} else {
		return nil, errors.New("error: failed to get key " + key + ", it's not in the skip list")
	}
}

// Update the element with the specified key by changing the item referenced by the skip list nodes. Return error if not
// present.
func (sl *SkipList) Update(key string, value []byte) error {
	resultNode := sl.searchForKey(key)
	if resultNode.item != nil && resultNode.item.Key == key {
		resultNode.item.Value = value
		return nil
	} else {
		return errors.New("error: failed to update element with key " + key + ", it's not in the skip list")
	}
}

func (sl *SkipList) Size() uint32 {
	return sl.size
}

// Add attempts to add a new item to the skip list. If they key is already present, updates the item.
// Returns error if the list is full.
func (sl *SkipList) Add(key string, value []byte) error {
	/*   x     o
		 x  x  o
		 o ox oo
	     oooxxoo
		 -------
		 124568nil
	*/ //Let's say this is our skip list. If we were searching for the 7, the search would follow the path marked by the
	//   x's. The rightmost x's in each row are the ones who may point to our new element, depending on the height of
	//   the new column. We will do the search twice, redirecting nodes on the second go.

	resultNode := sl.searchForKey(key)
	if resultNode.item != nil && resultNode.item.Key == key { // The key is already present, so we update it instead.
		resultNode.item.Key = key
		resultNode.item.Value = value
		return nil
	}
	if sl.size == sl.maxSize {
		return errors.New("error: failed to add item with key " + key + ", skip list is full")
	}
	sl.size++
	newItem := &item.Item{Key: key, Value: value}

	var newHeight uint32 = 1
	// We go to maxHeight-1, because then the first column will be at maxHeight
	for rand.Intn(2) == 1 && newHeight < sl.maxHeight-1 {
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
		if currentNode.next == nil || key < currentNode.next.item.Key {
			if currentHeight <= newHeight { // we are adding a new node
				currentNode.next = &skipListNode{currentNode.next, nil, newItem}
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

// Delete attempts to delete the item with the specified key from the skip list. Returns error if it's not present.
func (sl *SkipList) Delete(key string) error {
	resultNode := sl.searchForKey(key)
	if resultNode.item == nil || resultNode.item.Key != key {
		return errors.New("error: failed to delete key" + key + ", it's not in the skip list")
	} else {
		currentNode := sl.head
		for { // This for loop is structured a bit differently than other searches because we know the key is present.
			if currentNode.next == nil {
				currentNode = currentNode.down
			} else if key <= currentNode.next.item.Key {
				if key == currentNode.next.item.Key {
					currentNode.next.down = nil
					currentNode.next.item = nil
					currentNode.next = currentNode.next.next // The garbage collector will delete the old node
				}
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
		sl.size--

		return nil
	}
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
