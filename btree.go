package main

import (
	"bytes"
	"fmt"
	"sort"
)

func _assert(cond bool) {
	if cond == false {
		panic("Assertion failed")
	}
}

// ------------------------------- Node -------------------------------

type Bytes []byte

type INode struct {
	key   Bytes
	value Bytes
}

type INodes []INode
type Children []*Node

type Node struct {
	inodes   INodes
	children Children
}

func (a Bytes) less(b Bytes) bool {
	return bytes.Compare(a, b) == -1
}

func (a Bytes) equal(b Bytes) bool {
	return bytes.Equal(a, b)
}

// If found, return the found index.
// Otherwise, return the index to be inserted.
func (inodes INodes) find(key Bytes) (bool, int) {
	// See: go doc sort.Search. For the following
	// to work, items must be sorted.
	// If we search for 4 in:
	//   idx:    0 1 2 3
	//   values: 1 2 3 5
	// sort.Search() will return (index) 3
	// If we search for 3, it'll return 3 again
	// because 3 is not less than 3.
	// So, if the item does not exist, it'll return
	// the index of the next larger number. If it exists,
	// it'll return the index right after it. See `if`s below.
	idx := sort.Search(len(inodes), func(i int) bool {
		return key.less(inodes[i].key)
	})
	if idx > 0 && inodes[idx-1].key.equal(key) {
		return true, idx - 1
	}
	return false, idx
}

func insert_at[T any](sl []T, idx int, item T) []T {
	sl = append(sl, item)
	goes_on_the_back := (idx == len(sl))
	if goes_on_the_back {
		return sl
	}
	// Move suffix part one item forward
	//     dst         src
	copy(sl[idx+1:], sl[idx:])
	// Insert
	sl[idx] = item
	return sl
}

func (n *Node) insert_inode_and_split_if_needed(idx, max_inodes int, inode INode) (bool, INode, *Node) {
	empty_inode := INode{}
	n.inodes = insert_at(n.inodes, idx, inode)
	if n.should_split(max_inodes) {
		mid_inode, new_child := n.split_in_half()
		return true, mid_inode, new_child
	}
	return false, empty_inode, nil
}

func (n *Node) insert(inode INode, max_inodes int) (bool, INode, *Node) {
	found, idx := n.inodes.find(inode.key)
	empty_inode := INode{}
	if found {
		return false, empty_inode, nil
	}

	if n.is_leaf() {
		return n.insert_inode_and_split_if_needed(idx, max_inodes, inode)
	} else {
		was_split, mid_inode, new_child := n.children[idx].insert(inode, max_inodes)
		if was_split {
			n.children = insert_at(n.children, idx+1, new_child)
			return n.insert_inode_and_split_if_needed(idx, max_inodes, mid_inode)
		}
	}
	return false, empty_inode, nil
}

func (n *Node) is_leaf() bool {
	return len(n.children) == 0
}

// You should call this _after_ you have inserted.
func (n *Node) should_split(max_inodes int) bool {
	// It should be strictly less so that one more
	// inode fits.
	return len(n.inodes) == (max_inodes + 1)
}

func (n *Node) split_in_half() (INode, *Node) {
	_assert(len(n.inodes)%2 == 1)
	is_leaf := n.is_leaf()

	mid_idx := len(n.inodes) / 2
	mid_inode := n.inodes[mid_idx]
	new_node := new(Node)
	new_node.inodes = append(new_node.inodes, n.inodes[mid_idx+1:]...)
	// Truncate
	n.inodes = n.inodes[:mid_idx]

	// Move and truncate children too. But this is a subtle point.
	// Look at the intuition notes.
	if !is_leaf {
		// The first `mid_idx` children stay in `n` and the rest
		// go to `new_node`. All the `mid_idx` children are smaller
		// than `mid_inode`, and the rest are bigger. `mid_node` will
		// have as children `n` (on the left) and `new_inode`.
		new_node.children = append(new_node.children, n.children[mid_idx+1:]...)
		n.children = n.children[:mid_idx+1]
	}

	return mid_inode, new_node
}

// ------------------------------- BTree -------------------------------

// --- Subtle Intuitions in B-Trees ---
//
// I recommend staring at this image for a bit to see how re-balancing
// works in B-Trees: https://en.wikipedia.org/wiki/B-tree#/media/File:B_tree_insertion_example.png
//
// Notice that when we try to insert 4, we do _NOT_ insert it in the root, even though
// it has space, and even though 4 is bigger than 3. Insertions happen only
// at leaf nodes.
//
// Other notes:
// - A B-Tree grows upwards.
// - If a node is maxed out, say the max cap is N, then there are two cases:
// (a) It is a leaf node (no children), and we kept pushing to it.
// (b) It is not a leaf node. Then, it _must_ have at least N+1
//     children, and so we can, and should, split the children
//     in half too. In particular, notice that in a non-leaf node,
//     there can't be an k-th inode without a k-th and a k+1-th child, because
//     the only way the k-th inode occured is that one of the k first children
//     pushed an inode upwards. If it wasn't the k-th child, then we shifted
//     the children by one.
//
//     Thus, if a non-leaf node is maxed out, then we had at least N children, and one
//     pushed an inode upwards, making us have N+1 children.

type BTree struct {
	// There are many conflicting definitions
	// for the terms "order" and "order" for
	// trees in general and B-trees in particular.
	// Here, in a B-tree of order `d`, each node has:
	// - At least `d` keys and `d+1` children
	// - At most `2d` keys and `2d+1` children.
	// This follows the definition 2.1 of the original paper:
	// Organization And Maintenance Of Large Ordered Indices (https://doi.org/10.1145/1734663.1734671)
	// (which does not mention order or degree explicitly) and the later paper:
	// Ubiquitous B-Tree (https://dl.acm.org/doi/10.1145/356770.356776)
	// which mentions "order" explictly.
	order int
	root  *Node
}

func get_new_btree(order int) *BTree {
	if order < 1 {
		panic("Bad degree for BTree")
	}
	return &BTree{order: order, root: new(Node)}
}

func (tree *BTree) max_items_per_node() int {
	return tree.order * 2
}

func (tree *BTree) insert(key, value Bytes) {
	root := tree.root
	max_inodes := tree.max_items_per_node()
	_assert(root != nil)

	was_split, mid_inode, new_child := root.insert(INode{key, value}, max_inodes)
	if was_split {
		old_root := root
		new_root := new(Node)
		new_root.inodes = append(new_root.inodes, mid_inode)
		new_root.children = append(new_root.children, old_root, new_child)
		tree.root = new_root
	}
}

func (tree *BTree) find(key Bytes) (found bool, value Bytes) {
	runner := tree.root
	for {
		found, idx := runner.inodes.find(key)
		if found {
			return true, runner.inodes[idx].value
		}
		if runner.is_leaf() {
			return false, nil
		}
		runner = runner.children[idx]
		_assert(runner != nil)
	}
}

var indent_level int = 0

func print_node(n *Node) {

	print_indent := func() {
		for i := 0; i < indent_level; i++ {
			fmt.Print("    ")
		}
	}

	print_child := func(child *Node) {
		indent_level++
		print_node(child)
		indent_level--
	}
	// There is the possibility that a node has no
	// inodes. For example, if the order is 1, then
	// when we split a node in "half", we're left
	// with 1 elements on the left half, 1 middle element
	// and no right elements. So, the right node is empty.
	// It would be ugly to check this here, as we need to know
	// if somebody called us (and we need to set this argument
	// appropriately in the loop below that iterates children)
	_assert(n != nil)
	is_leaf := n.is_leaf()
	for idx, inode := range n.inodes {
		if !is_leaf {
			child := n.children[idx]
			print_child(child)
		}
		print_indent()
		fmt.Print("{", string(inode.key), ", ", string(inode.value), "}")
		fmt.Println()
	}
	if !is_leaf {
		print_child(n.children[len(n.children)-1])
	}
}

func (tree *BTree) print() {
	print_node(tree.root)
	fmt.Println("-----------------------")
}

func main() {
	t := get_new_btree(1)
	t.insert([]byte("10"), []byte("24"))
	t.print()
	t.insert([]byte("20"), []byte("38"))
	t.print()
	t.insert([]byte("30"), []byte("457"))
	t.print()
	t.insert([]byte("40"), []byte("7"))
	t.print()
	t.insert([]byte("50"), []byte("24"))
	t.print()
	t.insert([]byte("60"), []byte("32"))
	t.print()
	t.insert([]byte("70"), []byte("7"))
	t.print()
	t.insert([]byte("80"), []byte("7"))
	t.print()
	t.insert([]byte("0"), []byte("7"))
	t.print()
	t.insert([]byte("05"), []byte("7"))
	t.print()
	t.insert([]byte("35"), []byte("7"))
	t.print()
	t.insert([]byte("36"), []byte("7"))
	t.print()
}
