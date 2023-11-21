package merkle_tree

import (
	"crypto/sha1"
	hashFn "nasp-project/structures/hash"
)

type Node struct {
	left  Hashable
	right Hashable
}
type EmptyBlock struct {
}
type Hash [20]byte
type Block string

type MerkleTree struct {
	hash hashFn.SeededHash
}

func NewMerkleTree(values []Hashable) []Hashable {
	var nodes []Hashable

	for i := 1; i < len(values); i += 2 {
		if i+1 < len(values) {
			nodes = append(nodes, Node{left: values[i], right: values[i+1]})
		} else {
			nodes = append(nodes, Node{left: values[i], right: EmptyBlock{}})
		}
	}
	if len(nodes) == 1 {
		return nodes
	} else if len(nodes) > 1 {
		return NewMerkleTree(nodes)
	} else {
		panic("Error occurred")
	}
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
	return hash([]byte(b))
}
func (_ EmptyBlock) hash() Hash {
	return [20]byte{}
}
func hash(data []byte) Hash {
	return sha1.Sum(data)
}
