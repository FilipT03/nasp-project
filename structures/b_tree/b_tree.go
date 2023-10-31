package b_tree

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

func NewBTree(minItems int) *BTree {
	owner := &BTree{
		root: &Node{},
	}
	owner.root.owner = owner
	owner.minItems = minItems
	owner.maxItems = 2 * minItems
	return owner
}

func (bt *BTree) Find(key string) *Item {
	index, node, _ := bt.findKey(key)
	if index == -1 {
		return nil
	}
	return node.items[index]
}

func (bt *BTree) Add(key string, value []byte) {
	item := newItem(key, value)
	index, nodeToInsert, ancestors := bt.findKey(item.key)
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

func NewNode(owner *BTree, items []*Item, children []*Node) *Node {
	return &Node{
		owner:    owner,
		items:    items,
		children: children,
	}
}

func (bt *BTree) getPath(indexes []int) []*Node {
	nodes := []*Node{bt.root}
	child := bt.root

	for i := 1; i < len(indexes); i++ {
		child = child.children[indexes[i]]
		nodes = append(nodes, child)
	}
	return nodes
}

func (bt *BTree) findKey(key string) (int, *Node, []int) {
	current := bt.root
	ancestors := []int{0}

	for {
		found, index := current.findKey(key)
		if found || current.isLeaf() {
			return index, current, ancestors
		}
		nextChild := current.children[index]
		ancestors = append(ancestors, index)
		current = nextChild
	}
}

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

func (n *Node) isLeaf() bool {
	return len(n.children) == 0
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
