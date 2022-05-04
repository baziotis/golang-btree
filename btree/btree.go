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

type diskNodeIndex int32

type iNode struct {
	key   Bytes
	value Bytes
}

type iNodes []iNode
type children_t []diskNodeIndex

type node_t struct {
	inodes   iNodes
	children children_t
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
func (inodes iNodes) find(key Bytes) (bool, int) {
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

func (n *node_t) insert_inode_and_split_if_needed(inode iNode, idx int, parent *BTree, disk_node_idx diskNodeIndex) (bool, iNode, diskNodeIndex) {
	empty_inode := iNode{}
	n.inodes = insert_at(n.inodes, idx, inode)
	if n.should_split(parent.max_items_per_node()) {
		mid_inode, new_child := n.split_in_half(parent, disk_node_idx)
		return true, mid_inode, new_child
	} else {
		// Overwrite on disk
		parent.dnw.overwrite_disk_node(n, disk_node_idx)
	}
	return false, empty_inode, -1
}

func (n *node_t) insert(inode iNode, parent *BTree, disk_node_idx diskNodeIndex) (bool, iNode, diskNodeIndex) {
	found, idx := n.inodes.find(inode.key)
	empty_inode := iNode{}
	if found {
		return false, empty_inode, -1
	}

	if n.is_leaf() {
		return n.insert_inode_and_split_if_needed(inode, idx, parent, disk_node_idx)
	} else {
		child_disk_node_index := diskNodeIndex(n.children[idx])
		child_node := parent.dnw.get_node_from_idx(child_disk_node_index)
		was_split, mid_inode, new_child := child_node.insert(inode, parent, child_disk_node_index)
		if was_split {
			_assert(new_child != -1)
			n.children = insert_at(n.children, idx+1, new_child)
			return n.insert_inode_and_split_if_needed(mid_inode, idx, parent, disk_node_idx)
		}
	}
	return false, empty_inode, -1
}

func (n *node_t) is_leaf() bool {
	return len(n.children) == 0
}

// You should call this _after_ you have inserted.
func (n *node_t) should_split(max_inodes int) bool {
	// It should be strictly less so that one more
	// inode fits.
	return len(n.inodes) == (max_inodes + 1)
}

func (n *node_t) split_in_half(parent *BTree, node_idx diskNodeIndex) (iNode, diskNodeIndex) {
	_assert(len(n.inodes)%2 == 1)
	is_leaf := n.is_leaf()

	mid_idx := len(n.inodes) / 2
	mid_inode := n.inodes[mid_idx]
	new_node := new(node_t)
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

	// Overwrite the old node
	parent.dnw.overwrite_disk_node(n, node_idx)
	// Save new node to disk
	new_node_disk_idx := parent.dnw.save_new_node(new_node)

	return mid_inode, new_node_disk_idx
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

func (tree *BTree) Insert(key, value Bytes) {
	root := tree.dnw.get_root_node()
	_assert(root != nil)

	root_disk_node_index := diskNodeIndex(0)
	was_split, mid_inode, new_child := root.insert(iNode{key, value}, tree, root_disk_node_index)
	if was_split {
		old_root_new_idx := tree.dnw.move_root_in_new_disk_node()
		new_root := new(node_t)
		new_root.inodes = append(new_root.inodes, mid_inode)
		new_root.children = append(new_root.children, old_root_new_idx, new_child)
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
		found, idx := runner.inodes.find(key)
		if found {
			return true, runner.inodes[idx].value
		}
		if runner.is_leaf() {
			return false, nil
		}
		runner = tree.dnw.get_node_from_idx(runner.children[idx])
		_assert(runner != nil)
	}
}

var indent_level int = 0

func print_node(n *node_t, parent *BTree) {

	print_indent := func() {
		for i := 0; i < indent_level; i++ {
			fmt.Print("    ")
		}
	}

	print_child := func(child *node_t) {
		indent_level++
		print_node(child, parent)
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
			child := parent.dnw.get_node_from_idx(n.children[idx])
			print_child(child)
		}
		print_indent()
		fmt.Print("{", string(inode.key), ", ", string(inode.value), "}")
		fmt.Println()
	}
	if !is_leaf {
		last_child_node := parent.dnw.get_node_from_idx(n.children[len(n.children)-1])
		print_child(last_child_node)
	}
}

func (tree *BTree) Print() {
	print_node(tree.dnw.get_root_node(), tree)
	fmt.Println("-----------------------")
}

// ------------------------------- DiskNodeWriter -------------------------------

const START_FROM_THE_BEGINNING_OF_FILE = os.SEEK_SET

func get_node_from_disk_node(dnode *diskNode) *node_t {
	node := new(node_t)
	for i := 0; i < int(dnode.Num_key_values); i++ {
		key_value := dnode.Key_values[i]
		key_len := key_value.Key_len
		value_len := key_value.Value_len
		inode := iNode{key: key_value.Key[:key_len], value: key_value.Value[:value_len]}
		node.inodes = append(node.inodes, inode)
	}

	for i := 0; i < int(dnode.Num_children); i++ {
		child_idx := dnode.Children[i]
		node.children = append(node.children, child_idx)
	}

	return node
}

func get_disk_node_from_node(node *node_t) *diskNode {
	dnode := new(diskNode)
	_assert(len(node.inodes) <= MAX_KEY_VALUES)
	_assert(len(node.children) <= MAX_CHILDREN)
	dnode.Num_key_values = int32(len(node.inodes))
	dnode.Num_children = int32(len(node.children))

	for idx, inode := range node.inodes {
		_assert(len(inode.key) < MAX_KEY_LEN)
		_assert(len(inode.value) < MAX_VALUE_LEN)
		var dkv diskKeyValue
		dkv.Key_len = int32(len(inode.key))
		dkv.Value_len = int32(len(inode.value))

		copy(dkv.Key[:dkv.Key_len], inode.key)
		copy(dkv.Value[:dkv.Value_len], inode.value)

		dnode.Key_values[idx] = dkv
	}

	for idx, child_disk_idx := range node.children {
		dnode.Children[idx] = child_disk_idx
	}

	return dnode
}

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

// func main() {
// 	t := get_new_btree(1, "bt.db")
// 	t.insert([]byte("10"), []byte("24"))
// 	t.print()
// 	t.insert([]byte("20"), []byte("38"))
// 	t.print()
// 	t.insert([]byte("30"), []byte("457"))
// 	t.print()
// 	t.insert([]byte("40"), []byte("7"))
// 	t.print()
// 	t.insert([]byte("50"), []byte("24"))
// 	t.print()
// 	t.insert([]byte("60"), []byte("32"))
// 	t.print()
// 	t.insert([]byte("70"), []byte("7"))
// 	t.print()
// 	t.insert([]byte("80"), []byte("7"))
// 	t.print()
// 	t.insert([]byte("0"), []byte("7"))
// 	t.print()
// 	t.insert([]byte("05"), []byte("7"))
// 	t.print()
// 	t.insert([]byte("35"), []byte("7"))
// 	t.print()
// 	t.insert([]byte("36"), []byte("7"))
// 	t.print()

// 	t.close()
// }
