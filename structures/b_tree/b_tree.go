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

func (bt *BTree) Add(key string, value []byte) {
	item := newItem(key, value)
	_ = item
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

func newItem(key string, value []byte) *Item {
	return &Item{key: key, value: value}
}
