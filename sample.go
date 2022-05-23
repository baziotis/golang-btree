package main

import (
	"os"

	"github.com/baziotis/golang-btree/btree"
)

func _assert(cond bool) {
	if cond == false {
		panic("Assertion failed")
	}
}

func insert(key []byte, t *btree.BTree) {
	whatever_value := []byte("24")
	t.Insert(key, whatever_value)
}

func example1() {
	file := "bt.db"
	t := btree.GetNewBTree(1, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert([]byte("10"), t)
	insert([]byte("20"), t)
	insert([]byte("30"), t)
	insert([]byte("40"), t)
	insert([]byte("50"), t)
	insert([]byte("60"), t)
	insert([]byte("70"), t)
	insert([]byte("80"), t)
	insert([]byte("0"), t)
	insert([]byte("05"), t)
	insert([]byte("35"), t)
	insert([]byte("36"), t)
}

func delete_and_test_its_not_there(key []byte, t *btree.BTree) {
	print_values := false

	t.Print(print_values)
	_assert(t.Delete(key))
	found, _ := t.Find(key)
	_assert(!found)
	t.Print(print_values)
}

func deletion1() {
	file := "bt.db"
	t := btree.GetNewBTree(1, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert([]byte("10"), t)
	insert([]byte("20"), t)

	delete_and_test_its_not_there([]byte("10"), t)

	// Insert some keys to create leaves other
	// than the root.
	insert([]byte("10"), t)
	insert([]byte("30"), t)
	insert([]byte("40"), t)

	delete_and_test_its_not_there([]byte("40"), t)

	insert([]byte("40"), t)
	insert([]byte("50"), t)
	insert([]byte("60"), t)
	insert([]byte("15"), t)

	delete_and_test_its_not_there([]byte("30"), t)
}

func transfer_from_left() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert([]byte("10"), t)
	insert([]byte("20"), t)
	insert([]byte("30"), t)
	insert([]byte("40"), t)
	insert([]byte("50"), t)
	insert([]byte("13"), t)
	insert([]byte("15"), t)
	insert([]byte("17"), t)
	insert([]byte("14"), t)

	delete_and_test_its_not_there([]byte("20"), t)
}

func transfer_from_right() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert([]byte("10"), t)
	insert([]byte("20"), t)
	insert([]byte("30"), t)
	insert([]byte("40"), t)
	insert([]byte("50"), t)
	insert([]byte("13"), t)
	insert([]byte("15"), t)
	insert([]byte("17"), t)
	insert([]byte("18"), t)

	delete_and_test_its_not_there([]byte("10"), t)

	insert([]byte("45"), t)

	delete_and_test_its_not_there([]byte("18"), t)
}

func delete_with_merge() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert([]byte("10"), t)
	insert([]byte("20"), t)
	insert([]byte("30"), t)
	insert([]byte("40"), t)
	insert([]byte("50"), t)

	// Should borrow from left. It also makes
	// the parent empty, so this should replace
	// the parent, which is also the root.
	delete_and_test_its_not_there([]byte("40"), t)

	// Reset
	insert([]byte("40"), t)

	// Make the tree so that there are more than
	// one items on the parent (so that it doesn't
	// become empty when we take one)
	insert([]byte("13"), t)
	insert([]byte("15"), t)
	insert([]byte("17"), t)

	delete_and_test_its_not_there([]byte("17"), t)

	// Reset
	delete_and_test_its_not_there([]byte("13"), t)
	delete_and_test_its_not_there([]byte("15"), t)

	// Should borrow from right
	delete_and_test_its_not_there([]byte("10"), t)
}

func delete_with_merge2() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	// insert_and_print([]byte("10"), t)
	// insert_and_print([]byte("11"), t)
	// insert_and_print([]byte("12"), t)
	// insert_and_print([]byte("13"), t)
	// insert_and_print([]byte("14"), t)
	// insert_and_print([]byte("15"), t)
	// insert_and_print([]byte("16"), t)
	// insert_and_print([]byte("17"), t)
	// insert_and_print([]byte("18"), t)
	// insert_and_print([]byte("19"), t)
	// insert_and_print([]byte("20"), t)
	// insert_and_print([]byte("21"), t)
	// insert_and_print([]byte("22"), t)
	// insert_and_print([]byte("23"), t)
	// insert_and_print([]byte("24"), t)
	// insert_and_print([]byte("25"), t)
	// insert_and_print([]byte("26"), t)
	// insert_and_print([]byte("27"), t)
	// insert_and_print([]byte("28"), t)
	// insert_and_print([]byte("29"), t)
	// insert_and_print([]byte("30"), t)
	// insert_and_print([]byte("31"), t)
	// insert_and_print([]byte("32"), t)
	// insert_and_print([]byte("33"), t)
	// insert_and_print([]byte("34"), t)
	// insert_and_print([]byte("35"), t)
	// insert_and_print([]byte("36"), t)

	// Insert enough nodes to create parents that are not the
	// root
	insert([]byte("10"), t)
	insert([]byte("15"), t)
	insert([]byte("20"), t)
	insert([]byte("25"), t)
	insert([]byte("30"), t)
	insert([]byte("35"), t)
	insert([]byte("40"), t)
	insert([]byte("45"), t)
	insert([]byte("50"), t)
	insert([]byte("55"), t)
	insert([]byte("60"), t)
	insert([]byte("65"), t)
	insert([]byte("70"), t)
	insert([]byte("75"), t)
	insert([]byte("80"), t)
	insert([]byte("85"), t)
	insert([]byte("90"), t)

	delete_and_test_its_not_there([]byte("85"), t)
}

func transfer_from_right2() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert([]byte("10"), t)
	insert([]byte("15"), t)
	insert([]byte("20"), t)
	insert([]byte("25"), t)
	insert([]byte("30"), t)
	insert([]byte("35"), t)
	insert([]byte("40"), t)
	insert([]byte("45"), t)
	insert([]byte("50"), t)
	insert([]byte("55"), t)
	insert([]byte("60"), t)
	insert([]byte("65"), t)
	insert([]byte("70"), t)
	insert([]byte("75"), t)
	insert([]byte("80"), t)
	insert([]byte("85"), t)
	insert([]byte("90"), t)
	insert([]byte("91"), t)
	insert([]byte("92"), t)
	insert([]byte("93"), t)
	insert([]byte("94"), t)
	insert([]byte("95"), t)

	delete_and_test_its_not_there([]byte("30"), t)
}

func delete_internal() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert([]byte("10"), t)
	insert([]byte("20"), t)
	insert([]byte("30"), t)
	insert([]byte("40"), t)
	insert([]byte("50"), t)

	insert([]byte("11"), t)
	insert([]byte("16"), t)
	insert([]byte("13"), t)
	insert([]byte("12"), t)

	delete_and_test_its_not_there([]byte("13"), t)

	insert([]byte("13"), t)

	delete_and_test_its_not_there([]byte("12"), t)
}

func delete_internal_merge() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert([]byte("10"), t)
	insert([]byte("20"), t)
	insert([]byte("30"), t)
	insert([]byte("40"), t)
	insert([]byte("50"), t)

	insert([]byte("11"), t)
	insert([]byte("16"), t)
	insert([]byte("13"), t)

	delete_and_test_its_not_there([]byte("13"), t)
}

func delete_internal_merge2() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert([]byte("10"), t)
	insert([]byte("15"), t)
	insert([]byte("20"), t)
	insert([]byte("25"), t)
	insert([]byte("30"), t)
	insert([]byte("35"), t)
	insert([]byte("40"), t)
	insert([]byte("45"), t)
	insert([]byte("50"), t)
	insert([]byte("55"), t)
	insert([]byte("60"), t)
	insert([]byte("65"), t)
	insert([]byte("70"), t)
	insert([]byte("75"), t)
	insert([]byte("80"), t)
	insert([]byte("85"), t)
	insert([]byte("90"), t)

	delete_and_test_its_not_there([]byte("20"), t)
}

func main() {
	/// TODO: This is a little bit ridiculous now because the following functions
	/// act as tests, but they are _NOT_ automatic. You literally have
	/// to check manually that the trees changed as they are supposed
	/// to. Again, this is bad but I was bored of making a good solution.
	/// But here are some ideas:
	/// - The best alternative is to create a format, and parser, for a B-Tree.
	///   So, you write a B-Tree and parse it into an in-memory B-Tree. Then,
	///   you do a change into a B-Tree and you also provide what should be the
	///   the correct B-Tree. The one you're testing is already in-memory.
	///   The correct one is parsed into memory and they must be the same.
	///   Besides providing automation, the important thing in this solution
	///   is that you can _see_ the correct tree.
	/// - A more lazy approach is just create the equality checker (very simple).
	///   Then, you create the correct B-Tree by doing insertions accordingly.

	// deletion1()
	// transfer_from_left()
	// transfer_from_right()
	// delete_with_merge()
	// delete_with_merge2()
	// transfer_from_right2()
	// delete_internal()
	// delete_internal_merge()
	// delete_internal_merge2()
}
