package b_tree

import (
	"errors"
	"fmt"
	"nasp-project/model"
	"nasp-project/util"
)

type Node struct {
	owner    *BTree
	records  []*model.Record
	children []*Node
}

type BTree struct {
	root       *Node
	capacity   uint32
	size       uint32
	minRecords int
	maxRecords int
}

// NewBTree returns a new empty BTree instance.
func NewBTree(minRecords int) *BTree {
	owner := &BTree{
		root: &Node{},
	}
	owner.root.owner = owner
	owner.minRecords = minRecords
	owner.maxRecords = 2 * minRecords
	owner.capacity = uint32(util.GetConfig().MemTable.MaxSize)
	owner.size = 0
	return owner
}

// Get searches for a key in the B-tree and returns an error if the key is not found.
func (bt *BTree) Get(key []byte) (*model.Record, error) {
	index, node, _, found := bt.findKey(string(key), true)
	if !found {
		return nil, errors.New("error: key '" + string(key) + "' not found in B-tree")
	}
	return node.records[index], nil
}

// Add a new record to the B-tree while handling overflows.
func (bt *BTree) Add(record *model.Record) error {
	index, nodeToInsert, ancestors, found := bt.findKey(string(record.Key), false)
	_, err := nodeToInsert.addRecord(record, index)
	if err != nil {
		return err
	}
	nodePath := bt.getPath(ancestors)

	for i := len(nodePath) - 2; i >= 0; i-- {
		parentNode := nodePath[i]
		node := nodePath[i+1]
		nodeIndex := ancestors[i+1]
		if len(node.records) > bt.maxRecords {
			parentNode.split(node, nodeIndex)
		}
	}

	if len(bt.root.records) > bt.maxRecords {
		newRoot := NewNode(bt, []*model.Record{}, []*Node{bt.root})
		newRoot.split(bt.root, 0)
		bt.root = newRoot
	}

	if !found {
		bt.size++
	}
	return nil
}

// Delete marks a record as deleted by setting its tombstone to true.
// Returns an error if the key is not found or is already marked as deleted.
// Use Remove to delete key from the B tree.
func (bt *BTree) Delete(key []byte) error {
	index, nodeToInsert, _, found := bt.findKey(string(key), false)
	if !found {
		return errors.New("error: could not delete key '" + string(key) + "' as it does not exist B-tree")
	}
	if nodeToInsert.records[index].Tombstone {
		return errors.New("error: could not delete key '" + string(key) + "' as it is already deleted")
	}
	nodeToInsert.records[index].Tombstone = true
	return nil
}

// Remove deletes a key from the BTree. Returns an error if the key is not found.
// For logical deletion, use Delete.
func (bt *BTree) Remove(key []byte) error {
	index, nodeToDeleteFrom, ancestorsIndexes, found := bt.findKey(string(key), true)
	if !found {
		return errors.New("error: could not delete key '" + string(key) + "' as it does not exist B-tree")
	}
	if nodeToDeleteFrom.isLeaf() {
		nodeToDeleteFrom.records = append(nodeToDeleteFrom.records[:index], nodeToDeleteFrom.records[index+1:]...)
	} else {
		affectedNodes := make([]int, 0)
		affectedNodes = append(affectedNodes, index)

		childNode := nodeToDeleteFrom.children[index]
		for !childNode.isLeaf() {
			traverseIndex := len(childNode.children) - 1
			childNode = childNode.children[traverseIndex]
			affectedNodes = append(affectedNodes, traverseIndex)
		}
		nodeToDeleteFrom.records[index] = childNode.records[len(childNode.records)-1]
		childNode.records = childNode.records[:len(childNode.records)-1]

		ancestorsIndexes = append(ancestorsIndexes, affectedNodes...)
	}

	ancestors := bt.getPath(ancestorsIndexes)
	for i := len(ancestors) - 2; i >= 0; i-- {
		parentNode := ancestors[i]
		node := ancestors[i+1]
		if len(node.records) < bt.minRecords {
			parentNode.balance(ancestorsIndexes[i+1])
		}
	}

	if len(bt.root.records) == 0 && len(bt.root.children) > 0 {
		bt.root = bt.root.children[len(bt.root.children)-1]
	}

	bt.size--
	return nil
}

func getSortedNodes(node *Node) {
	for _, child := range node.children {
		getSortedNodes(child)
		fmt.Println(child)
	}
}

// Flush  returns sorted record in B tree
func (bt *BTree) Flush() []*model.Record {
	getSortedNodes(bt.root)
	return nil
}

func NewNode(owner *BTree, items []*model.Record, children []*Node) *Node {
	return &Node{
		owner:    owner,
		records:  items,
		children: children,
	}
}

// findKey returns index, node and ancestors of found key. Returns false if key is not found in B-tree.
func (bt *BTree) findKey(key string, exact bool) (int, *Node, []int, bool) {
	current := bt.root
	ancestors := []int{0}
	for {
		found, index := current.findKey(key)
		if found {
			return index, current, ancestors, true
		}
		if current.isLeaf() {
			if exact {
				return index, current, ancestors, false
			}
			return index, current, ancestors, false
		}
		nextChild := current.children[index]
		ancestors = append(ancestors, index)
		current = nextChild
	}
}

// findKey attempts to locate a key in the Node and returns the index where the key should be inserted if not found.
func (n *Node) findKey(key string) (bool, int) {
	for i, record := range n.records {
		if key == string(record.Key) {
			return true, i
		}
		if key < string(record.Key) {
			return false, i
		}
	}
	return false, len(n.records)
}

// getPath converts node indexes into Node pointers.
func (bt *BTree) getPath(indexes []int) []*Node {
	nodes := []*Node{bt.root}
	child := bt.root

	for i := 1; i < len(indexes); i++ {
		child = child.children[indexes[i]]
		nodes = append(nodes, child)
	}
	return nodes
}

// balance handles underflow by performing rotation (rotateRight, rotateLeft) and merging operations.
func (n *Node) balance(unbalancedNodeIndex int) {
	parentNode := n
	unbalancedNode := parentNode.children[unbalancedNodeIndex]

	var leftNode *Node
	if unbalancedNodeIndex != 0 {
		leftNode = parentNode.children[unbalancedNodeIndex-1]
		if len(leftNode.records) > n.owner.minRecords {
			rotateRight(leftNode, parentNode, unbalancedNode, unbalancedNodeIndex)
			return
		}
	}

	var rightNode *Node
	if unbalancedNodeIndex != len(parentNode.children)-1 {
		rightNode = parentNode.children[unbalancedNodeIndex+1]
		if len(rightNode.records) > n.owner.minRecords {
			rotateLeft(unbalancedNode, parentNode, rightNode, unbalancedNodeIndex)
			return
		}
	}

	merge(parentNode, unbalancedNodeIndex)
}

func rotateRight(leftNode *Node, parentNode *Node, unbalancedNode *Node, unbalancedNodeIndex int) {
	leftNodeItem := leftNode.records[len(leftNode.records)-1]
	leftNode.records = leftNode.records[:len(leftNode.records)-1]

	parentNodeItemIndex := unbalancedNodeIndex - 1
	if unbalancedNodeIndex == 0 {
		parentNodeItemIndex = 0
	}
	parentNodeItem := parentNode.records[parentNodeItemIndex]
	parentNode.records[parentNodeItemIndex] = leftNodeItem

	unbalancedNode.records = append([]*model.Record{parentNodeItem}, unbalancedNode.records...)

	if !leftNode.isLeaf() {
		childToShift := leftNode.children[len(leftNode.children)-1]
		leftNode.children = leftNode.children[:len(leftNode.children)-1]
		unbalancedNode.children = append([]*Node{childToShift}, unbalancedNode.children...)
	}
}

func rotateLeft(unbalancedNode *Node, parentNode *Node, rightNode *Node, unbalancedNodeIndex int) {
	rightNodeItem := rightNode.records[0]
	rightNode.records = rightNode.records[1:]

	parentNodeIndex := unbalancedNodeIndex
	if parentNodeIndex == len(parentNode.records) {
		parentNodeIndex = len(parentNode.records) - 1
	}
	parentNodeItem := parentNode.records[parentNodeIndex]
	parentNode.records[parentNodeIndex] = rightNodeItem

	unbalancedNode.records = append(unbalancedNode.records, parentNodeItem)

	if !unbalancedNode.isLeaf() {
		childToShift := rightNode.children[0]
		rightNode.children = unbalancedNode.children[1:]
		unbalancedNode.children = append(unbalancedNode.children, childToShift)
	}

}

func merge(parentNode *Node, unbalancedNodeIndex int) {
	unbalancedNode := parentNode.children[unbalancedNodeIndex]
	if unbalancedNodeIndex == 0 {
		rightNode := parentNode.children[unbalancedNodeIndex+1]

		parentNodeItem := parentNode.records[0]
		parentNode.records = parentNode.records[1:]
		unbalancedNode.records = append(unbalancedNode.records, parentNodeItem)

		unbalancedNode.records = append(unbalancedNode.records, rightNode.records...)
		parentNode.children = append(parentNode.children[0:1], parentNode.children[2:]...)

		if !rightNode.isLeaf() {
			unbalancedNode.children = append(unbalancedNode.children, rightNode.children...)
		}
	} else {
		leftNode := parentNode.children[unbalancedNodeIndex-1]

		parentNodeItem := parentNode.records[unbalancedNodeIndex-1]
		parentNode.records = append(parentNode.records[:unbalancedNodeIndex-1], parentNode.records[unbalancedNodeIndex:]...)
		leftNode.records = append(leftNode.records, parentNodeItem)

		leftNode.records = append(leftNode.records, unbalancedNode.records...)
		parentNode.children = append(parentNode.children[:unbalancedNodeIndex], parentNode.children[unbalancedNodeIndex+1:]...)

		if !leftNode.isLeaf() {
			unbalancedNode.children = append(leftNode.children, unbalancedNode.children...)
		}
	}
}

func (n *Node) addRecord(record *model.Record, index int) (int, error) {
	if n.owner.size == n.owner.capacity {
		return -1, errors.New("error: failed to add item with key " + string(record.Key) + ", skip list is full")
	}

	if len(n.records) == index {
		n.records = append(n.records, record)
		return index, nil
	}

	n.records = append(n.records[:index+1], n.records[index:]...)
	n.records[index] = record

	return index, nil
}

// split handles overflow by dividing a node into two nodes.
func (n *Node) split(modifiedNode *Node, index int) {
	minSize := n.owner.minRecords
	childIndex := 0
	for len(modifiedNode.records) > n.owner.maxRecords {
		middleItem := modifiedNode.records[minSize]
		var newNode *Node
		if modifiedNode.isLeaf() {
			newNode = NewNode(n.owner, modifiedNode.records[minSize+1:], []*Node{})
			modifiedNode.records = modifiedNode.records[:minSize]
		} else {
			newNode = NewNode(n.owner, modifiedNode.records[minSize+1:], modifiedNode.children[childIndex+1:])
			modifiedNode.records = modifiedNode.records[:minSize]
			modifiedNode.children = modifiedNode.children[:minSize+1]
		}
		_, _ = n.addRecord(middleItem, index)
		if len(n.children) == index+1 {
			n.children = append(n.children, newNode)
		} else {
			n.children = append(n.children[:childIndex+1], n.children[index:]...)
			n.children[index+1] = newNode
		}
		index += 1
		childIndex += 1
		modifiedNode = newNode
	}
}

func (n *Node) isLeaf() bool {
	return len(n.children) == 0
}
