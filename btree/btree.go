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

func tern[T any](pred bool, if_true T, if_false T) T {
	if pred {
		return if_true
	} else {
		return if_false
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
type nodeUniqueTag int32

type keyValue struct {
	key   Bytes
	value Bytes
}

type keyValues []keyValue
type children_t []nodeUniqueTag

type node_t struct {
	key_values    keyValues
	children_tags children_t
}

type taggedNode struct {
	node_t
	tag nodeUniqueTag
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
type diskNodeIndex nodeUniqueTag

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

// It preserves order, i.e., it does _not_ treat
// the slice as a set.
func pop_at[T any](sl []T, idx int) ([]T, T) {
	// If we didn't want to preserve order, we could
	// just replace the item to-be-deleted with the last
	// item and then shrink the slice by one.
	// But we have do sth better.
	len_ := len(sl)
	if len_ == 0 {
		panic("Tried to delete from empty slice")
	}
	_assert(idx >= 0 && idx < len_)

	// Save the item to return it
	item := sl[idx]

	new_len := len_ - 1
	new_sl := make([]T, new_len)
	// Declaring new_sl with: var new_sl []T
	// and then appending the two parts is more
	// readable but probably slower.
	copy(new_sl[:idx], sl[:idx])
	copy(new_sl[idx:new_len], sl[idx+1:len_])

	return new_sl, item
}

func pop_last[T any](sl []T) ([]T, T) {
	len_ := len(sl)
	if len_ == 0 {
		panic("Tried to pop last of empty slice")
	}
	return pop_at(sl, len_-1)
}

func pop_first[T any](sl []T) ([]T, T) {
	len_ := len(sl)
	if len_ == 0 {
		panic("Tried to pop last of empty slice")
	}
	return pop_at(sl, 0)
}

func delete_at[T any](sl []T, idx int) []T {
	sl, _ = pop_at(sl, idx)
	return sl
}

func (n *taggedNode) insert_key_value_and_split_if_needed(key_value keyValue, idx int, parent_tree *BTree) (bool, keyValue, nodeUniqueTag) {
	empty_key_value := keyValue{}
	n.key_values = insert_at(n.key_values, idx, key_value)
	if n.should_split(parent_tree.max_items_per_node()) {
		mid_key_value, new_child := n.split_in_half(parent_tree)
		return true, mid_key_value, new_child
	} else {
		// Overwrite on disk
		parent_tree.overwrite_node(n)
	}
	return false, empty_key_value, -1
}

func (n *taggedNode) insert(key_value keyValue, parent_tree *BTree) (bool, keyValue, nodeUniqueTag) {
	found, idx := n.key_values.find(key_value.key)
	empty_key_value := keyValue{}
	if found {
		return false, empty_key_value, -1
	}

	if n.is_leaf() {
		return n.insert_key_value_and_split_if_needed(key_value, idx, parent_tree)
	} else {
		child_tag := nodeUniqueTag(n.children_tags[idx])
		child_node := parent_tree.get_node_from_tag(child_tag)
		was_split, mid_key_value, new_child := child_node.insert(key_value, parent_tree)
		if was_split {
			_assert(new_child != -1)
			n.children_tags = insert_at(n.children_tags, idx+1, new_child)
			return n.insert_key_value_and_split_if_needed(mid_key_value, idx, parent_tree)
		}
	}
	return false, empty_key_value, -1
}

func (n *taggedNode) can_delete_one(parent_tree *BTree) bool {
	// Strictly larger because we'll delete one.
	return len(n.key_values) > parent_tree.min_items_per_node()
}

func borrow_from_sibling(n *taggedNode, sibling *taggedNode, where_to_pop func([]keyValue) ([]keyValue, keyValue), idx_of_middle int, idx_of_key int, where_to_insert int, parent_node *taggedNode, parent_tree *BTree) {
	// Take a key from the sibling. If it is the left sibling, we take its last
	// key. If it is the right, we take its first key. Make this key middle key in the parent.
	// Put the previous middle key in the place of the key to delete.
	// Example. Suppose we want to remove 31 below. `x` means empty.
	//        30|33
	//       /  |   \
	//      /   |    \
	// 25|28   31|x   ...
	//
	// Take 28, move it in place of 30, and move 30 in place of 31
	//
	// However, there's a sublety. If the key to delete is not the first
	// key, then the simple replacement will be wrong. In trees of order 1
	// the key to delete is always the first one, because there's
	// only one if the node underflows. But consider for instance, this
	// tree of order 2 (I've not included the empty spots):
	//          15|30|
	//         /  |   \
	//        /   |    \
	// 10|13|14  17|20   40|50|
	//
	// Say we delete 20. The middle node underflows, but its left sibling
	// has enough nodes to borrow. We can move 14 in place of 15 and use
	// 15 to have enough items in the middle node. But, we should _not_
	// put it in place of 20. Instead, we should basically push all nodes
	// by one forward (in this case, only 17) and put 15 first. This
	// "putting it first" always work because the middle key in the parent
	// is always smaller than everything in the node.

	// Pop the item from the sibling
	temp := sibling.key_values
	temp, borrowed_kv := where_to_pop(temp)
	sibling.key_values = temp

	// Get the middle key
	middle := parent_node.key_values[idx_of_middle]

	// Place the new middle
	parent_node.key_values[idx_of_middle] = borrowed_kv
	// Replace the key to be deleted
	// Note: This could be done in one go with less moving
	// around but I don't want to overcomplicate it.
	n.key_values = delete_at(n.key_values, idx_of_key)
	n.key_values = insert_at(n.key_values, where_to_insert, middle)

	// Write the 3 nodes to disk
	parent_tree.overwrite_node(sibling)
	parent_tree.overwrite_node(parent_node)
	parent_tree.overwrite_node(n)
}

type borrowingFrom int

const (
	BORROWING_FROM_LEFT borrowingFrom = iota
	BORROWING_FROM_RIGHT
)

func delete_merge(n *taggedNode, sibling *taggedNode, parent_node *taggedNode, idx_of_middle, idx_of_key, idx_in_parent int, borrowing_from borrowingFrom, parent_tree *BTree) {
	where_to_insert_idx := tern(borrowing_from == BORROWING_FROM_LEFT, 0, len(n.key_values))

	// Delete the key
	n.key_values = delete_at(n.key_values, idx_of_key)
	// Delete middle and insert it key as the first/last of `n`'s KVs.
	middle := parent_node.key_values[idx_of_middle]
	parent_node.key_values = delete_at(parent_node.key_values, idx_of_middle)
	n.key_values = insert_at(n.key_values, where_to_insert_idx, middle)
	if borrowing_from == BORROWING_FROM_LEFT {
		// Append `n`'s KVs to the end of (left) sibling
		sibling.key_values = append(sibling.key_values, n.key_values...)
	} else {
		// Append (right) sibling's KVs to the end of `n` and make that the new
		// sibling.
		sibling.key_values = append(n.key_values, sibling.key_values...)
	}

	// Remove `n` as a child of parent_node
	parent_node.children_tags = delete_at(parent_node.children_tags, idx_in_parent)
	// Delete `n`.
	parent_tree.delete_node(n)

	// If the parent is left with no items then we should delete the parent.
	// Basicall, then the new node (i.e., sibling) takes its place.
	// To do that, we just take its tag. Note: This handles the case
	// where the parent is the root.
	if len(parent_node.key_values) == 0 {
		sibling.tag = parent_node.tag
		parent_tree.delete_node(parent_node)
		parent_tree.overwrite_node(sibling)
	} else {
		// Overwrite left_sibling and parent_node
		parent_tree.overwrite_node(sibling)
		parent_tree.overwrite_node(parent_node)
	}
}

// Delete a key-value. We assume it exists in `n`. `idx_of_key` is the idx, in `n`, where we find
// the key-value pair to delete. `idx_in_parent` is the index of `n` in `parent_node.children`.
//
// Note: We could compute `idx_in_parent` on demand by searching n.tag in parent_node.children
func (n *taggedNode) delete(idx_of_key int, parent_node *taggedNode, idx_in_parent int, parent_tree *BTree) {
	if n.is_leaf() {
		is_root := (parent_node == nil)
		if is_root || n.can_delete_one(parent_tree) {
			n.key_values = delete_at(n.key_values, idx_of_key)
			parent_tree.overwrite_node(n)
			return
		}
		// Otherwise, it underflows

		// Useful later.
		is_first_child := (idx_in_parent == 0)
		is_last_child := (idx_in_parent == int(len(parent_node.children_tags))-1)

		// Find the index of the middle key that connects
		// us with our sibling. It's different if we're talking
		// about the left or the right sibling. See borrow_from_sibling()
		// to see why the middle key is useful.

		//
		// Left
		// Note the -1. This is because this is not the first child.
		idx_of_middle_left := idx_in_parent - 1
		//
		// Right
		// Dealing with children being one more than items.
		idx_of_middle_right := tern(is_first_child, 0, idx_in_parent)
		// First, check if left sibling exists and won't underflow if we delete one
		left_sibling_exists := !is_first_child
		if left_sibling_exists {
			left_sibling := parent_tree.get_node_from_tag(parent_node.children_tags[idx_in_parent-1])
			if left_sibling.can_delete_one(parent_tree) {

				where_to_insert := 0 // i.e., first
				borrow_from_sibling(n, left_sibling, pop_last[keyValue],
					idx_of_middle_left, idx_of_key,
					where_to_insert, parent_node,
					parent_tree)

				return
			}
		}

		// Otherwise, try to borrow from the right sibling, if it exists.
		right_sibling_exists := !is_last_child
		if right_sibling_exists {
			right_sibling := parent_tree.get_node_from_tag(parent_node.children_tags[idx_in_parent+1])
			if right_sibling.can_delete_one(parent_tree) {
				// Similar to the left sibling; a little weirder
				// in finding the idx of the middle key.

				where_to_insert := len(n.key_values) // i.e., last
				borrow_from_sibling(n, right_sibling, pop_first[keyValue],
					idx_of_middle_right, idx_of_key,
					where_to_insert, parent_node,
					parent_tree)

				return
			}
		}

		// Otherwise, merge the node with the left sibling, if it exists, otherwise
		// merge it with the right sibling. We _are_ able to do that,
		// because to be here, both the left and the right siblings have
		// the minimum number of keys (if they exist).
		// Reminder: Either a left or a right sibling _must_ exist (except
		// if this is the root but we have checked that), because the only
		// way a parent node is created is that a node got full and was split
		// in two.
		if left_sibling_exists {
			left_sibling := parent_tree.get_node_from_tag(parent_node.children_tags[idx_in_parent-1])
			delete_merge(n, left_sibling, parent_node,
				idx_of_middle_left, idx_of_key,
				idx_in_parent, BORROWING_FROM_LEFT, parent_tree)

			return
		}
		// See above
		_assert(right_sibling_exists)

		right_sibling := parent_tree.get_node_from_tag(parent_node.children_tags[idx_in_parent+1])
		delete_merge(n, right_sibling, parent_node,
			idx_of_middle_right, idx_of_key,
			idx_in_parent, BORROWING_FROM_RIGHT, parent_tree)

		return
	} else {
		_assert(false)
	}
	return
}

func (n *node_t) is_leaf() bool {
	return len(n.children_tags) == 0
}

// You should call this _after_ you have inserted.
func (n *node_t) should_split(max_key_values int) bool {
	// It should be strictly less so that one more
	// key_value fits.
	return len(n.key_values) == (max_key_values + 1)
}

func (n *taggedNode) split_in_half(parent_tree *BTree) (keyValue, nodeUniqueTag) {
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
		new_node.children_tags = append(new_node.children_tags, n.children_tags[mid_idx+1:]...)
		n.children_tags = n.children_tags[:mid_idx+1]
	}

	// Overwrite the old node
	parent_tree.overwrite_node(n)
	// Save new node and get an id
	new_node_id := parent_tree.save_new_node(new_node)

	return mid_key_value, new_node_id
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

func (tree *BTree) max_children() int {
	return tree.max_items_per_node() + 1
}

func (tree *BTree) Insert(key, value Bytes) {
	tagged_root := tree.get_root_node()
	_assert(tagged_root != nil)

	was_split, mid_key_value, new_child := tagged_root.insert(keyValue{key, value}, tree)
	if was_split {
		old_root_new_id := tree.move_root_to_new_node()
		new_root := new(node_t)
		new_root.key_values = append(new_root.key_values, mid_key_value)
		new_root.children_tags = append(new_root.children_tags, old_root_new_id, new_child)
		tree.dnw.overwrite_disk_node(new_root, 0)
	}
}

func (tree *BTree) Close() {
	err := tree.dnw.file.Close()
	panic_on_err(err)
}

// If the key is found, we return the node at which the key is, the parent
// of this node, and the  index of the key in this node.
func (tree *BTree) find(key Bytes) (found bool, node *taggedNode, idx_of_key int, parent *taggedNode, idx_in_parent int) {
	parent = nil
	idx_in_parent = -1
	runner := tree.get_root_node()
	for {
		found, idx := runner.key_values.find(key)
		if found {
			idx_of_key = idx
			return true, runner, idx_of_key, parent, idx_in_parent
		}
		if runner.is_leaf() {
			return false, nil, -1, nil, -1
		}
		parent = runner
		idx_in_parent = idx
		runner = tree.get_node_from_tag(runner.children_tags[idx])
		_assert(runner != nil)
	}
}

func (tree *BTree) Find(key Bytes) (found bool, value Bytes) {
	found, node, idx_of_key, _, _ := tree.find(key)
	if found {
		return true, node.key_values[idx_of_key].value
	}
	return false, nil
}

func (tree *BTree) Delete(key Bytes) (found bool) {
	found, node, idx_of_key, parent, idx_in_parent := tree.find(key)
	if !found {
		return false
	}
	node.delete(idx_of_key, parent, idx_in_parent, tree)
	return true
}

var indent_level int = 0

func print_node(n *taggedNode, parent_tree *BTree, print_values bool) {
	// We may be able to use Traverse() instead
	// rewriting the traverse code but who cares...
	// We'd need to pass the current level of the tree too.

	print_indent := func() {
		for i := 0; i < indent_level; i++ {
			fmt.Print("    ")
		}
	}

	print_child := func(child *taggedNode) {
		indent_level++
		print_node(child, parent_tree, print_values)
		indent_level--
	}
	_assert(n != nil)
	is_leaf := n.is_leaf()
	for idx, key_value := range n.key_values {
		if !is_leaf {
			child := parent_tree.get_node_from_tag(n.children_tags[idx])
			print_child(child)
		}
		print_indent()
		fmt.Print("{", string(key_value.key))
		if print_values {
			fmt.Print(", ", string(key_value.value), "}")
		} else {
			fmt.Print("}")
		}
		fmt.Println()
	}
	if !is_leaf {
		last_child_node := parent_tree.get_node_from_tag(n.children_tags[len(n.children_tags)-1])
		print_child(last_child_node)
	}
}

func (tree *BTree) Print(print_values bool) {
	print_node(tree.get_root_node(), tree, print_values)
	fmt.Println("-----------------------")
}

func (tree *BTree) Traverse(f func(*node_t, *BTree)) {
	root := tree.get_root_node()
	var traverse_node func(n *node_t)

	traverse_node = func(n *node_t) {
		_assert(n != nil)
		// Apply the caller's function
		f(n, tree)
		is_leaf := n.is_leaf()
		for _, child_id := range n.children_tags {
			if !is_leaf {
				child := tree.get_node_from_tag(child_id)
				traverse_node(&child.node_t)
			}
		}
	}

	traverse_node(&root.node_t)
}

/* The following are a thin layer over DiskNodeWriter. The idea is
that node_t that uses BTree to save nodes, does not need to know where
we save them */

func (tree *BTree) overwrite_node(n *taggedNode) {
	overwriting_root := (n.tag == 0)
	if !overwriting_root {
		_assert(check_node_invariants(&n.node_t, tree))
	}
	tree.dnw.overwrite_disk_node(&n.node_t, diskNodeIndex(n.tag))
}

func (tree *BTree) delete_node(n *taggedNode) {
	// Normally, we would add `n` to a free-list of nodes,
	// so that it can be reused. But who cares for now...
	return
}

// When we save a node, we return back a unique identifier for it.
func (tree *BTree) save_new_node(n *node_t) nodeUniqueTag {
	tag := nodeUniqueTag(tree.dnw.save_new_node(n))
	wrote_root := (tag == 0)
	if !wrote_root {
		_assert(check_node_invariants(n, tree))
	}
	// Should we also set n.id = id here?
	return tag
}

func (tree *BTree) get_node_from_tag(tag nodeUniqueTag) *taggedNode {
	n := tree.dnw.get_node_from_idx(diskNodeIndex(tag))
	tagged_node := taggedNode{*n, tag}
	_assert(check_node_invariants(n, tree))
	return &tagged_node
}

func (tree *BTree) move_root_to_new_node() nodeUniqueTag {
	return nodeUniqueTag(tree.dnw.move_root_in_new_disk_node())
}

func (tree *BTree) get_root_node() *taggedNode {
	root := tree.dnw.get_root_node()
	tagged_root := &taggedNode{*root, nodeUniqueTag(0)}
	return tagged_root
}

func get_node_from_disk_node(dnode *diskNode, disk_idx diskNodeIndex) *node_t {
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
		node.children_tags = append(node.children_tags, nodeUniqueTag(child_idx))
	}

	return node
}

func get_disk_node_from_node(node *node_t) *diskNode {
	dnode := new(diskNode)
	_assert(len(node.key_values) <= MAX_KEY_VALUES)
	_assert(len(node.children_tags) <= MAX_CHILDREN)
	dnode.Num_key_values = int32(len(node.key_values))
	dnode.Num_children = int32(len(node.children_tags))

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

	for idx, child_id := range node.children_tags {
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

	return get_node_from_disk_node(dnode, idx)
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
