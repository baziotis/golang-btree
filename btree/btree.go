package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"reflect"
	"sort"
)

func _assert(cond bool) {
	if cond == false {
		panic("Assertion failed")
	}
}

func panic_on_err(err error) {
	if err != nil {
		panic(err)
	}
}

func check_file_exists(path string) bool {
	_, err := os.Stat(path)
	// It's weird that you check it like that, but I found no better solution.
	file_exists := (err == nil)
	return file_exists
}

// Yes, go requires weird shit like that because
// unsafe.Sizeof() doesn't work on a type, only
// an expression.
func get_sizeof_type[T any]() uintptr {
	ty := reflect.TypeOf((*T)(nil)).Elem()
	return ty.Size()
}

type Bytes []byte

// This is used as a unique way of identifying nodes. Under the hood, for now,
// these are disk node indices, but the node_t methods don't need to know
// how we identify nodes and why.
//
// It may be better to save this inside node_t instead of passing it around.
type nodeUniqueIdentifier int32

type keyValue struct {
	key   Bytes
	value Bytes
}

type keyValues []keyValue
type children_t []nodeUniqueIdentifier

type node_t struct {
	key_values keyValues
	children   children_t
}

const MAX_KEY_LEN = 20
const MAX_VALUE_LEN = 20

type diskKeyValue struct {
	Key_len   int32
	Key       [MAX_KEY_LEN]byte
	Value_len int32
	Value     [MAX_VALUE_LEN]byte
}

// A node as saved and read from disk. Used only as
// an intermediary when saving and loading a Node.
// In a real implementation, we should try to make
// a DiskNode be as close as possible to an OS page size.

const MAX_KEY_VALUES = 20
const MAX_CHILDREN = MAX_KEY_VALUES + 1

// The field names need to be in caps so that they are exported to other
// modules (no, I am not kidding). We need that so that binary.Read()
// and binary.Write() can work with them. One alternative is to read
// each field individually but this is error prone as you might forget one.
type diskNode struct {
	Num_key_values int32
	Num_children   int32
	Key_values     [MAX_KEY_VALUES]diskKeyValue
	Children       [MAX_CHILDREN]diskNodeIndex
}
type diskNodeIndex nodeUniqueIdentifier

// The structure of a file is 4 bytes (i.e., sizeof(DiskNodeIndex))
// which denote the number of nodes, and then an array of DiskNode.
// The first DiskNode is always the root.
//
// Btw, we need to know the number of nodes already in the file, so that
// we can recover the B-Tree. The most important thing in recovering a B-Tree
// is to know: (a) whether a root exists in the file (b) where it is. We can solve
// (a) by checking if the file exists (generally, we assume that if it exists, it
// has a btree in it) and we solve (b) by putting the root always in index 0 (offset 4).
//
// But, whenever we create a new node, we put it at the end of the file. We node to know
// what this end is, and this is where the number of nodes helps.
// TBH, we could rewrite that by just doing a division with sizeof(DiskNode) and avoid
// the 4 bytes but anyway. In any case, TODO: it would be good to save the order too.

type diskNodeWriter struct {
	file     *os.File
	next_idx diskNodeIndex
}

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
//     there can't be an k-th key_value without a k-th and a k+1-th child, because
//     the only way the k-th key_value occured is that one of the k first children
//     pushed a key_value upwards. If it wasn't the k-th child, then we shifted
//     the children by one.
//
//     Thus, if a non-leaf node is maxed out, then we had at least N children, and one
//     pushed a key_value upwards, making us have N+1 children.

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
	dnw   diskNodeWriter
}

// ------------------------------- Node -------------------------------

func (a Bytes) less(b Bytes) bool {
	return bytes.Compare(a, b) == -1
}

func (a Bytes) equal(b Bytes) bool {
	return bytes.Equal(a, b)
}

// If found, return the found index.
// Otherwise, return the index to be inserted.
func (key_values keyValues) find(key Bytes) (bool, int) {
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
	idx := sort.Search(len(key_values), func(i int) bool {
		return key.less(key_values[i].key)
	})
	if idx > 0 && key_values[idx-1].key.equal(key) {
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

func delete_at[T any](sl []T, idx int) []T {
	last_idx := len(sl) - 1
	// Swap the last item with the one to delete
	sl[last_idx], sl[idx] = sl[idx], sl[last_idx]
	// Shrink the slice by one
	// TODO: It seems that Go doesn't really have a way to shrink a slice
	// and free the surplus memory. The closest thing is to allocate
	// a new slice and copy the elements into it, but why complicate
	// the code. This is Go after all...
	sl = sl[:last_idx]
	return sl
}

func (n *node_t) insert_key_value_and_split_if_needed(key_value keyValue, idx int, parent_tree *BTree, disk_node_idx nodeUniqueIdentifier) (bool, keyValue, nodeUniqueIdentifier) {
	empty_key_value := keyValue{}
	n.key_values = insert_at(n.key_values, idx, key_value)
	if n.should_split(parent_tree.max_items_per_node()) {
		mid_key_value, new_child := n.split_in_half(parent_tree, disk_node_idx)
		return true, mid_key_value, new_child
	} else {
		// Overwrite on disk
		parent_tree.overwrite_node(n, disk_node_idx)
	}
	return false, empty_key_value, -1
}

func (n *node_t) insert(key_value keyValue, parent_tree *BTree, disk_node_idx nodeUniqueIdentifier) (bool, keyValue, nodeUniqueIdentifier) {
	found, idx := n.key_values.find(key_value.key)
	empty_key_value := keyValue{}
	if found {
		return false, empty_key_value, -1
	}

	if n.is_leaf() {
		return n.insert_key_value_and_split_if_needed(key_value, idx, parent_tree, disk_node_idx)
	} else {
		child_disk_node_index := nodeUniqueIdentifier(n.children[idx])
		child_node := parent_tree.get_node_from_id(child_disk_node_index)
		was_split, mid_key_value, new_child := child_node.insert(key_value, parent_tree, child_disk_node_index)
		if was_split {
			_assert(new_child != -1)
			n.children = insert_at(n.children, idx+1, new_child)
			return n.insert_key_value_and_split_if_needed(mid_key_value, idx, parent_tree, disk_node_idx)
		}
	}
	return false, empty_key_value, -1
}

func (n *node_t) is_leaf() bool {
	return len(n.children) == 0
}

// You should call this _after_ you have inserted.
func (n *node_t) should_split(max_key_values int) bool {
	// It should be strictly less so that one more
	// key_value fits.
	return len(n.key_values) == (max_key_values + 1)
}

func (n *node_t) split_in_half(parent_tree *BTree, node_idx nodeUniqueIdentifier) (keyValue, nodeUniqueIdentifier) {
	_assert(len(n.key_values)%2 == 1)
	is_leaf := n.is_leaf()

	mid_idx := len(n.key_values) / 2
	mid_key_value := n.key_values[mid_idx]
	new_node := new(node_t)
	new_node.key_values = append(new_node.key_values, n.key_values[mid_idx+1:]...)
	// Truncate
	n.key_values = n.key_values[:mid_idx]

	// Move and truncate children too. But this is a subtle point.
	// Look at the intuition notes.
	if !is_leaf {
		// The first `mid_idx` children stay in `n` and the rest
		// go to `new_node`. All the `mid_idx` children are smaller
		// than `mid_key_value`, and the rest are bigger. `mid_node` will
		// have as children `n` (on the left) and `new_key_value`.
		new_node.children = append(new_node.children, n.children[mid_idx+1:]...)
		n.children = n.children[:mid_idx+1]
	}

	// Overwrite the old node
	parent_tree.overwrite_node(n, node_idx)
	// Save new node to disk
	new_node_disk_idx := parent_tree.save_new_node(new_node)

	return mid_key_value, new_node_disk_idx
}

// Make sure that a node has at least min number of key_values and at most max.
// You should not run this on the root.
func check_node_invariants(n *node_t, parent_tree *BTree) bool {
	num_key_values := len(n.key_values)
	at_least_min := num_key_values >= parent_tree.min_items_per_node()
	at_most_max := num_key_values <= parent_tree.max_items_per_node()
	if !at_least_min || !at_most_max {
		fmt.Println(n.is_leaf(), num_key_values, parent_tree.order)
		return false
	}
	return true
}

// ------------------------------- BTree -------------------------------

func GetNewBTree(order int, path string) *BTree {
	if order < 1 {
		panic("Bad order for BTree")
	}
	btree := &BTree{order: order}
	if btree.max_items_per_node() > MAX_KEY_VALUES {
		panic("Too large order")
	}
	file_exists, dnw := get_new_disk_node_writer(path)
	if !file_exists {
		// Write a root because a root should always exist
		// in the file.
		root := new(node_t)
		dnw.save_new_node(root)
	}
	btree.dnw = dnw

	return btree
}

func (tree *BTree) max_items_per_node() int {
	return tree.order * 2
}

func (tree *BTree) min_items_per_node() int {
	return tree.order
}

func (tree *BTree) Insert(key, value Bytes) {
	root := tree.dnw.get_root_node()
	_assert(root != nil)

	// TODO: We may want to save the disk node index at the node,
	// and not carrying it around.
	root_disk_node_index := nodeUniqueIdentifier(0)
	was_split, mid_key_value, new_child := root.insert(keyValue{key, value}, tree, root_disk_node_index)
	if was_split {
		old_root_new_id := tree.move_root_to_new_node()
		new_root := new(node_t)
		new_root.key_values = append(new_root.key_values, mid_key_value)
		new_root.children = append(new_root.children, old_root_new_id, new_child)
		tree.dnw.overwrite_disk_node(new_root, 0)
		// tree.root = new_root
	}
}

func (tree *BTree) Close() {
	err := tree.dnw.file.Close()
	panic_on_err(err)
}

func (tree *BTree) Find(key Bytes) (found bool, value Bytes) {
	runner := tree.dnw.get_root_node()
	for {
		found, idx := runner.key_values.find(key)
		if found {
			return true, runner.key_values[idx].value
		}
		if runner.is_leaf() {
			return false, nil
		}
		runner = tree.get_node_from_id(runner.children[idx])
		_assert(runner != nil)
	}
}

var indent_level int = 0

func print_node(n *node_t, parent_tree *BTree) {
	// We may be able to use Traverse() instead
	// rewriting the traverse code but who cares...
	// We'd need to pass the current level of the tree too.

	print_indent := func() {
		for i := 0; i < indent_level; i++ {
			fmt.Print("    ")
		}
	}

	print_child := func(child *node_t) {
		indent_level++
		print_node(child, parent_tree)
		indent_level--
	}
	_assert(n != nil)
	is_leaf := n.is_leaf()
	for idx, key_value := range n.key_values {
		if !is_leaf {
			child := parent_tree.get_node_from_id(n.children[idx])
			print_child(child)
		}
		print_indent()
		fmt.Print("{", string(key_value.key), ", ", string(key_value.value), "}")
		fmt.Println()
	}
	if !is_leaf {
		last_child_node := parent_tree.get_node_from_id(n.children[len(n.children)-1])
		print_child(last_child_node)
	}
}

func (tree *BTree) Print() {
	print_node(tree.dnw.get_root_node(), tree)
	fmt.Println("-----------------------")
}

func (tree *BTree) Traverse(f func(*node_t, *BTree)) {
	root := tree.dnw.get_root_node()
	var traverse_node func(n *node_t)

	traverse_node = func(n *node_t) {
		_assert(n != nil)
		// Apply the caller's function
		f(n, tree)
		is_leaf := n.is_leaf()
		for _, child_id := range n.children {
			if !is_leaf {
				child := tree.get_node_from_id(child_id)
				traverse_node(child)
			}
		}
	}

	traverse_node(root)
}

/* The following are a thin layer over DiskNodeWriter. The idea is
that node_t that uses BTree to save nodes, does not need to know where
we save them */

func (tree *BTree) overwrite_node(n *node_t, id nodeUniqueIdentifier) {
	overwriting_root := (id == 0)
	if !overwriting_root {
		_assert(check_node_invariants(n, tree))
	}
	tree.dnw.overwrite_disk_node(n, diskNodeIndex(id))
}

func (tree *BTree) save_new_node(n *node_t) nodeUniqueIdentifier {
	id := nodeUniqueIdentifier(tree.dnw.save_new_node(n))
	wrote_root := (id == 0)
	if !wrote_root {
		_assert(check_node_invariants(n, tree))
	}
	return id
}

func (tree *BTree) get_node_from_id(id nodeUniqueIdentifier) *node_t {
	n := tree.dnw.get_node_from_idx(diskNodeIndex(id))
	_assert(check_node_invariants(n, tree))
	return n
}

func (tree *BTree) move_root_to_new_node() nodeUniqueIdentifier {
	return nodeUniqueIdentifier(tree.dnw.move_root_in_new_disk_node())
}

func get_node_from_disk_node(dnode *diskNode) *node_t {
	node := new(node_t)
	for i := 0; i < int(dnode.Num_key_values); i++ {
		dnode_key_value := dnode.Key_values[i]
		key_len := dnode_key_value.Key_len
		value_len := dnode_key_value.Value_len
		node_key_value := keyValue{key: dnode_key_value.Key[:key_len], value: dnode_key_value.Value[:value_len]}
		node.key_values = append(node.key_values, node_key_value)
	}

	for i := 0; i < int(dnode.Num_children); i++ {
		child_idx := dnode.Children[i]
		node.children = append(node.children, nodeUniqueIdentifier(child_idx))
	}

	return node
}

func get_disk_node_from_node(node *node_t) *diskNode {
	dnode := new(diskNode)
	_assert(len(node.key_values) <= MAX_KEY_VALUES)
	_assert(len(node.children) <= MAX_CHILDREN)
	dnode.Num_key_values = int32(len(node.key_values))
	dnode.Num_children = int32(len(node.children))

	for idx, key_value := range node.key_values {
		_assert(len(key_value.key) < MAX_KEY_LEN)
		_assert(len(key_value.value) < MAX_VALUE_LEN)
		var dkv diskKeyValue
		dkv.Key_len = int32(len(key_value.key))
		dkv.Value_len = int32(len(key_value.value))

		copy(dkv.Key[:dkv.Key_len], key_value.key)
		copy(dkv.Value[:dkv.Value_len], key_value.value)

		dnode.Key_values[idx] = dkv
	}

	for idx, child_id := range node.children {
		dnode.Children[idx] = diskNodeIndex(child_id)
	}

	return dnode
}

// ------------------------------- DiskNodeWriter -------------------------------

const START_FROM_THE_BEGINNING_OF_FILE = os.SEEK_SET

func (dnw *diskNodeWriter) get_node_from_idx(idx diskNodeIndex) *node_t {
	offset := get_offset_from_idx(idx)
	_, err := dnw.file.Seek(offset, START_FROM_THE_BEGINNING_OF_FILE)
	panic_on_err(err)

	dnode := new(diskNode)
	err = binary.Read(dnw.file, binary.LittleEndian, dnode)
	panic_on_err(err)

	return get_node_from_disk_node(dnode)
}

func (dnw *diskNodeWriter) get_root_node() *node_t {
	idx_of_root := diskNodeIndex(0)
	return dnw.get_node_from_idx(idx_of_root)
}

func get_offset_from_idx(idx diskNodeIndex) int64 {
	sizeof_DiskNode := int64(get_sizeof_type[diskNode]())
	sizeof_Num_nodes := int64(get_sizeof_type[diskNodeIndex]())
	// +4 because we have bytes at the start of the file which
	// denote the number of nodes.
	// TODO: We should also save the order in the file.
	offset := int64(idx)*sizeof_DiskNode + sizeof_Num_nodes
	return offset
}

func (dnw *diskNodeWriter) write_node_to_disk(node *node_t, idx diskNodeIndex) {
	// Get DiskNode
	dnode := get_disk_node_from_node(node)
	// Set offset
	offset := get_offset_from_idx(idx)
	_, err := dnw.file.Seek(offset, START_FROM_THE_BEGINNING_OF_FILE)
	panic_on_err(err)

	// Write
	err = binary.Write(dnw.file, binary.LittleEndian, dnode)
	panic_on_err(err)
}

func (dnw *diskNodeWriter) save_new_node(node *node_t) diskNodeIndex {
	dnw.write_node_to_disk(node, dnw.next_idx)
	node_idx := dnw.next_idx
	dnw.next_idx++
	// Save the number of nodes in the first 4 bytes of the file
	offset := int64(0)
	dnw.file.Seek(offset, START_FROM_THE_BEGINNING_OF_FILE)
	binary.Write(dnw.file, binary.LittleEndian, &dnw.next_idx)
	return node_idx
}

func (dnw *diskNodeWriter) overwrite_disk_node(node *node_t, idx diskNodeIndex) {
	_assert(idx < dnw.next_idx)
	dnw.write_node_to_disk(node, idx)
}

// The root is special. We don't care where are the rest of the nodes,
// because we can find them from the root. But the root should always
// be in a known index so that when we load a file, we know where's
// the root of the tree.
// So, when we update the root, we copy the old root to some other place,
// and save the new one at 0.
func (dnw *diskNodeWriter) move_root_in_new_disk_node() (new_idx diskNodeIndex) {
	root := dnw.get_node_from_idx(0)
	new_idx = dnw.save_new_node(root)
	return
}

func get_new_disk_node_writer(path string) (file_exists bool, dnw diskNodeWriter) {
	file_exists = check_file_exists(path)
	var permissions os.FileMode = 0666
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, permissions)
	panic_on_err(err)

	next_idx := diskNodeIndex(0)
	if file_exists {
		// Read the number of nodes in the file and set next_idx
		binary.Read(file, binary.LittleEndian, &next_idx)
	} else {
		// Write 0 as the number of nodes
		// Just to be sure, set the offset
		file.Seek(0, START_FROM_THE_BEGINNING_OF_FILE)
		binary.Write(file, binary.LittleEndian, &next_idx)
	}

	// We should save in the file the number of entries.
	// Now, we always start with an empty file.
	dnw = diskNodeWriter{file: file, next_idx: next_idx}
	return
}
