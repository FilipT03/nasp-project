package merkle_tree

// data

import (
	"crypto/sha1"
	"fmt"
	"io"
	hashFn "nasp-project/structures/hash"
	"nasp-project/structures/sstable"
	"os"
)

type Node struct {
	left  Hashable
	right Hashable
	value Hashable
}

// EmptyBlock struct to represent empty block
// it will be used when we have odd number of blocks in []hashable for Merkle tree
type EmptyBlock struct {
}

type Hash [20]byte

type Block []byte

// MerkleTree struct to represent Merkle tree
// it will contain hash function and root node
type MerkleTree struct {
	hash     hashFn.SeededHash
	root     Hashable
	nodeList []Hashable
}

// NewMerkleTree Function to create new Merkle tree from SSTable
// It will read all data from SSTable and hash them
// and then create Merkle tree from hashed data
// main function to use
func NewMerkleTree(ssTable sstable.SSTable, chunkSize int) MerkleTree {
	var hashedData []Hashable
	dataPath := ssTable.Data.Filename
	indexPath := ssTable.Index.Filename
	summaryPath := ssTable.Summary.Filename
	filterPath := ssTable.Filter.Filename
	// read data from file
	hData := readFile(dataPath, chunkSize)
	hashedData = append(hashedData, hData...)
	// read index from file
	hIndex := readFile(indexPath, chunkSize)
	hashedData = append(hashedData, hIndex...)
	// read summary from file
	hSummary := readFile(summaryPath, chunkSize)
	hashedData = append(hashedData, hSummary...)
	// read filter from file
	hFilter := readFile(filterPath, chunkSize)
	hashedData = append(hashedData, hFilter...)

	root := _createMerkleTree(hashedData)
	merkleTree := MerkleTree{
		hash:     hashFn.NewSeededHash(1),
		root:     _createMerkleTree(hashedData),
		nodeList: levelOrderTraversal(root),
	}
	return merkleTree
}

// LevelOrderTraversal function to traverse Merkle tree level by level
// it will return list of nodes in level order
// used for saving merkle tree to a file
func levelOrderTraversal(root Hashable) []Hashable {
	if root == nil {
		return nil
	}
	var queue = make([]Hashable, 0)
	var nodeList = make([]Hashable, 0)
	// Push to the queue
	queue = append(queue, root)
	// Loop until queue is empty
	for len(queue) > 0 {
		// Top (just get next element, don't remove it)
		// if my top is a node, then add it to the list
		// if I got all to the bottom my leafs will be added to the list
		// they are not node type they are Hash type, and they must be added to the list
		top := queue[0]
		if node, ok := queue[0].(Node); ok {
			nodeList = append(nodeList, node.value)
			// Pop
			queue = queue[1:]
		} else if node, ok := queue[0].(Hash); ok {
			nodeList = append(nodeList, node)
			// Pop
			queue = queue[1:]
			continue
		} else {
			break
		}

		if node, ok := top.(Node); ok {
			if node.left != nil {
				queue = append(queue, node.left)
			}
			if node.right != nil {
				queue = append(queue, node.right)
			}
		}

	}
	return nodeList
}

// readFile function to read file chunk by chunk
// and hash them immediately, so we don't have to store them in memory
func readFile(path string, chunkSize int) []Hashable {
	// return hashed data
	// will read every file chunk by chunk and hash them immediately
	var hashedData []Hashable
	file, err := os.Open(path)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	//defer file.Close()

	for {
		chunk := make([]byte, chunkSize)
		n, err := file.Read(chunk)
		if err == io.EOF {
			if n != 0 {
				hashedData = append(hashedData, hash(chunk[:n]))
			}
			break
		} else if err != nil {
			fmt.Println("Error opening file:", err)
			return nil
		}
		hashedData = append(hashedData, hash(chunk))
	}
	return hashedData
}

// _createMerkleTree function to create Merkle tree from hashed data
// it will create nodes from hashed data
func _createMerkleTree(values []Hashable) Hashable {
	var nodes []Hashable

	for i := 0; i < len(values); i += 2 {
		if i+1 < len(values) {
			v1 := values[i].hash()
			v2 := values[i+1].hash()
			concatenatedData := append(v1[:], v2[:]...)
			hashedData := hash(concatenatedData)
			nodes = append(nodes, Node{left: values[i], right: values[i+1], value: hashedData})
		} else {
			v1 := values[i].hash()
			v2 := EmptyBlock{}.hash()
			concatenatedData := append(v1[:], v2[:]...)
			hashedData := hash(concatenatedData)
			nodes = append(nodes, Node{left: values[i], right: EmptyBlock{}, value: hashedData})
		}
	}
	if len(nodes) == 1 {
		return nodes[0]
	} else if len(nodes) > 1 {
		return _createMerkleTree(nodes)
	} else {
		panic("Error occurred")
	}
}

// Contains function to check if Merkle tree contains a node
// it will check if root is equal to node, and then
// it will check if any of the children are equal to node using _dfs function
func (merkle MerkleTree) Contains(node Hashable) bool {
	root := merkle.root
	if root == node {
		return true
	}

	return _dfs(root, node)
}

// _dfs function to check if any of the children are equal to node
// it will check if left child is equal to node, and then
// it will check if right child is equal to node recursively
func _dfs(root Hashable, node Hashable) bool {
	if root == nil {
		return false
	}

	if rootNode, ok := root.(Node); ok {
		leftChild := rootNode.left
		rightChild := rootNode.right
		if leftChild == node || rightChild == node {
			return true
		} else if rootNode == node {
			return true
		}

		return _dfs(leftChild, node) || _dfs(rightChild, node)
	}
	return false
}

// Equal check two merkle trees are equal and
// return all different nodes or nil if they are equal
func (merkle MerkleTree) Equal(other MerkleTree) []Hashable {

	return _equalNodes(merkle.root.(Node), other.root.(Node))
}

// _equalNodes function to check if two nodes are equal
// it will check if left child is equal to node, and then
// it will check if right child is equal to node recursively
func _equalNodes(merkleNode Node, otherNode Node) []Hashable {
	if merkleNode.hash() == otherNode.hash() {
		return nil
	}
	if merkleNode.hash() != otherNode.hash() {
		return []Hashable{otherNode}
	}
	return append(_equalNodes(merkleNode.left.(Node), otherNode.left.(Node)), _equalNodes(merkleNode.right.(Node), merkleNode.right.(Node))...)
}

type Hashable interface {
	hash() Hash
}

func (n Node) hash() Hash {
	var l, r [sha1.Size]byte
	l = n.left.hash()

	r = n.right.hash()
	return hash(append(l[:], r[:]...))
}

func (b Block) hash() Hash {
	return hash([]byte(b)[:])
}

func (EmptyBlock) hash() Hash {
	return [20]byte{}
}

func (h Hash) hash() Hash {
	return sha1.Sum(h[:])
}

func hash(data []byte) Hash {
	return sha1.Sum(data)
}
