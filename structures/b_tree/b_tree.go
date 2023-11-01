package b_tree

import "errors"

type Item struct {
	key   string
	value []byte
}

type Node struct {
	owner    *BTree
	items    []*Item
	children []*Node
}

type BTree struct {
	root     *Node
	minItems int
	maxItems int
}

// NewBTree returns a new empty BTree instance.
func NewBTree(minItems int) *BTree {
	owner := &BTree{
		root: &Node{},
	}
	owner.root.owner = owner
	owner.minItems = minItems
	owner.maxItems = 2 * minItems
	return owner
}

// Find searches key in B tree.
func (bt *BTree) Find(key string) ([]byte, error) {
	index, node, _ := bt.findKey(key, true)
	if index == -1 {
		return nil, errors.New("error: key'" + "key" + "' not found in B tree")
	}
	return node.items[index].value, nil
}

// Add adds new key into B tree while handling overflows.
func (bt *BTree) Add(key string, value []byte) {
	item := newItem(key, value)
	index, nodeToInsert, ancestors := bt.findKey(item.key, false)
	nodeToInsert.addItem(item, index)
	nodePath := bt.getPath(ancestors)

	for i := len(nodePath) - 2; i >= 0; i-- {
		parentNode := nodePath[i]
		node := nodePath[i+1]
		nodeIndex := ancestors[i+1]
		if len(node.items) > bt.maxItems {
			parentNode.split(node, nodeIndex)
		}
	}

	if len(bt.root.items) > bt.maxItems {
		newRoot := NewNode(bt, []*Item{}, []*Node{bt.root})
		newRoot.split(bt.root, 0)
		bt.root = newRoot
	}
}

// Delete deletes key from B tree. Returns error if key is not found.
func (bt *BTree) Delete(key string) error {
	index, nodeToDeleteFrom, ancestorsIndexes := bt.findKey(key, true)
	if index == -1 {
		return errors.New("error: key '" + "key" + "' not found in B tree")
	}
	if nodeToDeleteFrom.isLeaf() {
		nodeToDeleteFrom.items = append(nodeToDeleteFrom.items[:index], nodeToDeleteFrom.items[index+1:]...)
	} else {
		affectedNodes := make([]int, 0)
		affectedNodes = append(affectedNodes, index)

		childNode := nodeToDeleteFrom.children[index]
		for !childNode.isLeaf() {
			traverseIndex := len(childNode.children) - 1
			childNode = childNode.children[traverseIndex]
			affectedNodes = append(affectedNodes, traverseIndex)
		}
		nodeToDeleteFrom.items[index] = childNode.items[len(childNode.items)-1]
		childNode.items = childNode.items[:len(childNode.items)-1]

		ancestorsIndexes = append(ancestorsIndexes, affectedNodes...)
	}

	ancestors := bt.getPath(ancestorsIndexes)
	for i := len(ancestors) - 2; i >= 0; i-- {
		parentNode := ancestors[i]
		node := ancestors[i+1]
		if len(node.items) < bt.minItems {
			parentNode.balance(ancestorsIndexes[i+1])
		}
	}

	return nil
}

func NewNode(owner *BTree, items []*Item, children []*Node) *Node {
	return &Node{
		owner:    owner,
		items:    items,
		children: children,
	}
}

// getPath converts nodeIndexes into *Node.
func (bt *BTree) getPath(indexes []int) []*Node {
	nodes := []*Node{bt.root}
	child := bt.root

	for i := 1; i < len(indexes); i++ {
		child = child.children[indexes[i]]
		nodes = append(nodes, child)
	}
	return nodes
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

// findKey tries to find a key in Node. If not found, returns index where key should be.
func (n *Node) findKey(key string) (bool, int) {
	for i, item := range n.items {
		if key == item.key {
			return true, i
		}
		if key < item.key {
			return false, i
		}
	}
	return false, len(n.items)
}

// balance handles underflow - left rotation, right rotation and merge.
func (n *Node) balance(unbalancedNodeIndex int) {
	parentNode := n
	unbalancedNode := parentNode.children[unbalancedNodeIndex]

	var leftNode *Node
	if unbalancedNodeIndex != 0 {
		leftNode = parentNode.children[unbalancedNodeIndex-1]
		if len(leftNode.items) > n.owner.minItems {
			rotateRight(leftNode, parentNode, unbalancedNode, unbalancedNodeIndex)
			return
		}
	}

	var rightNode *Node
	if unbalancedNodeIndex != 0 {
		rightNode = parentNode.children[unbalancedNodeIndex+1]
		if len(leftNode.items) > n.owner.minItems {
			rotateLeft(unbalancedNode, parentNode, rightNode, unbalancedNodeIndex)
			return
		}
	}

	merge(parentNode, unbalancedNodeIndex)
}

func rotateRight(leftNode *Node, parentNode *Node, unbalancedNode *Node, unbalancedNodeIndex int) {
	leftNodeItem := leftNode.items[len(leftNode.items)-1]
	leftNode.items = leftNode.items[:len(leftNode.items)-1]

	parentNodeItemIndex := unbalancedNodeIndex - 1
	if unbalancedNodeIndex == 0 {
		parentNodeItemIndex = 0
	}
	parentNodeItem := parentNode.items[parentNodeItemIndex]
	parentNode.items[parentNodeItemIndex] = leftNodeItem

	unbalancedNode.items = append([]*Item{parentNodeItem}, unbalancedNode.items...)

	if !leftNode.isLeaf() {
		childToShift := leftNode.children[len(leftNode.children)-1]
		leftNode.children = leftNode.children[:len(leftNode.children)-1]
		unbalancedNode.children = append([]*Node{childToShift}, unbalancedNode.children...)
	}
}

func rotateLeft(unbalancedNode *Node, parentNode *Node, rightNode *Node, unbalancedNodeIndex int) {
	rightNodeItem := rightNode.items[0]
	rightNode.items = rightNode.items[1:]

	parentNodeIndex := unbalancedNodeIndex
	if parentNodeIndex == len(parentNode.items) {
		parentNodeIndex = len(parentNode.items) - 1
	}
	parentNodeItem := parentNode.items[parentNodeIndex]
	parentNode.items[parentNodeIndex] = rightNodeItem

	unbalancedNode.items = append(unbalancedNode.items, parentNodeItem)

	if unbalancedNode.isLeaf() {
		childToShift := rightNode.children[0]
		rightNode.children = unbalancedNode.children[1:]
		unbalancedNode.children = append(unbalancedNode.children, childToShift)
	}

}

func merge(parentNode *Node, unbalancedNodeIndex int) {
	unbalancedNode := parentNode.children[unbalancedNodeIndex]
	if unbalancedNodeIndex == 0 {
		rightNode := parentNode.children[unbalancedNodeIndex+1]

		parentNodeItem := parentNode.items[0]
		parentNode.items = parentNode.items[1:]
		unbalancedNode.items = append(unbalancedNode.items, parentNodeItem)

		unbalancedNode.items = append(unbalancedNode.items, rightNode.items...)
		parentNode.children = append(parentNode.children[0:1], parentNode.children[2:]...)

		if !rightNode.isLeaf() {
			unbalancedNode.children = append(unbalancedNode.children, rightNode.children...)
		}
	} else {
		leftNode := parentNode.children[unbalancedNodeIndex-1]

		parentNodeItem := parentNode.items[unbalancedNodeIndex-1]
		parentNode.items = append(parentNode.items[:unbalancedNodeIndex-1], parentNode.items[unbalancedNodeIndex:]...)
		leftNode.items = append(leftNode.items, parentNodeItem)

		leftNode.items = append(leftNode.items, unbalancedNode.items...)
		parentNode.children = append(parentNode.children[:unbalancedNodeIndex], parentNode.children[unbalancedNodeIndex+1:]...)

		if !leftNode.isLeaf() {
			unbalancedNode.children = append(leftNode.children, unbalancedNode.children...)
		}
	}
}

func (n *Node) isLeaf() bool {
	return n.children == nil
}

func (n *Node) addItem(item *Item, index int) int {
	if len(n.items) == index {
		n.items = append(n.items, item)
		return index
	}

	n.items = append(n.items[:index+1], n.items[index:]...)
	n.items[index] = item
	return index
}

// split handles overflow.
func (n *Node) split(modifiedNode *Node, index int) {
	minSize := n.owner.minItems
	childIndex := 0
	for len(modifiedNode.items) > n.owner.maxItems {
		middleItem := modifiedNode.items[minSize]
		var newNode *Node
		if modifiedNode.isLeaf() {
			newNode = NewNode(n.owner, modifiedNode.items[minSize+1:], []*Node{})
			modifiedNode.items = modifiedNode.items[:minSize]
		} else {
			newNode = NewNode(n.owner, modifiedNode.items[minSize+1:], modifiedNode.children[childIndex+1:])
			modifiedNode.items = modifiedNode.items[:minSize]
			modifiedNode.children = modifiedNode.children[:minSize+1]
		}
		n.addItem(middleItem, index)
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

func newItem(key string, value []byte) *Item {
	return &Item{key: key, value: value}
}
