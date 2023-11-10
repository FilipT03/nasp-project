package b_tree

import (
	"errors"
	"nasp-project/util"
)

// TODO: Add BTree size and capacity

type Node struct {
	owner    *BTree
	records  []*util.DataRecord
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
	return owner
}

// Get searches for a key in the B-tree and returns an error if the key is not found.
func (bt *BTree) Get(key string) (*util.DataRecord, error) {
	index, node, _ := bt.findKey(key, true)
	if index == -1 {
		return nil, errors.New("error: key '" + key + "' not found in B-tree")
	}
	return node.records[index], nil
}

// Add a new record to the B-tree while handling overflows.
func (bt *BTree) Add(key string, value []byte) error {
	record := util.NewRecord([]byte(key), value)
	index, nodeToInsert, ancestors := bt.findKey(string(record.Key), false)
	nodeToInsert.addRecord(record, index)
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
		newRoot := NewNode(bt, []*util.DataRecord{}, []*Node{bt.root})
		newRoot.split(bt.root, 0)
		bt.root = newRoot
	}
	return nil
}

// Delete removes a key from the BTree. It returns an error if the key is not found.
func (bt *BTree) Delete(key string) error {
	index, nodeToDeleteFrom, ancestorsIndexes := bt.findKey(key, true)
	if index == -1 {
		return errors.New("error: could not delete key '" + key + "' as it does not exist B-tree")
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

	return nil
}

func NewNode(owner *BTree, items []*util.DataRecord, children []*Node) *Node {
	return &Node{
		owner:    owner,
		records:  items,
		children: children,
	}
}

// findKey returns index, node and ancestors of found key. If `exact` is true, it returns index -1 if key is not found.
func (bt *BTree) findKey(key string, exact bool) (int, *Node, []int) {
	current := bt.root
	ancestors := []int{0}
	for {
		found, index := current.findKey(key)
		if found {
			return index, current, ancestors
		}
		if current.isLeaf() {
			if exact {
				return -1, nil, nil
			}
			return index, current, ancestors
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

	unbalancedNode.records = append([]*util.DataRecord{parentNodeItem}, unbalancedNode.records...)

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

func (n *Node) addRecord(item *util.DataRecord, index int) int {
	if len(n.records) == index {
		n.records = append(n.records, item)
		return index
	}

	n.records = append(n.records[:index+1], n.records[index:]...)
	n.records[index] = item
	return index
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
		n.addRecord(middleItem, index)
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
