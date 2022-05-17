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

func insert_and_print(key []byte, t *btree.BTree) {
	print_values := false

	whatever_value := []byte("24")
	t.Insert(key, whatever_value)
	t.Print(print_values)
}

func example1() {
	file := "bt.db"
	t := btree.GetNewBTree(1, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("20"), t)
	insert_and_print([]byte("30"), t)
	insert_and_print([]byte("40"), t)
	insert_and_print([]byte("50"), t)
	insert_and_print([]byte("60"), t)
	insert_and_print([]byte("70"), t)
	insert_and_print([]byte("80"), t)
	insert_and_print([]byte("0"), t)
	insert_and_print([]byte("05"), t)
	insert_and_print([]byte("35"), t)
	insert_and_print([]byte("36"), t)
}

func delete_and_test_its_not_there(key []byte, t *btree.BTree) {
	print_values := false

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

	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("20"), t)

	delete_and_test_its_not_there([]byte("10"), t)

	// Insert some keys to create leaves other
	// than the root.
	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("30"), t)
	insert_and_print([]byte("40"), t)

	delete_and_test_its_not_there([]byte("40"), t)

	insert_and_print([]byte("40"), t)
	insert_and_print([]byte("50"), t)
	insert_and_print([]byte("60"), t)
	insert_and_print([]byte("15"), t)

	delete_and_test_its_not_there([]byte("30"), t)
}

func borrow_from_left() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("20"), t)
	insert_and_print([]byte("30"), t)
	insert_and_print([]byte("40"), t)

	delete_and_test_its_not_there([]byte("10"), t)

	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("50"), t)
	insert_and_print([]byte("13"), t)
	insert_and_print([]byte("15"), t)
	insert_and_print([]byte("17"), t)
	insert_and_print([]byte("14"), t)

	delete_and_test_its_not_there([]byte("20"), t)
}

func borrow_from_right() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("20"), t)
	insert_and_print([]byte("30"), t)
	insert_and_print([]byte("40"), t)

	delete_and_test_its_not_there([]byte("10"), t)

	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("50"), t)
	insert_and_print([]byte("13"), t)
	insert_and_print([]byte("15"), t)
	insert_and_print([]byte("17"), t)
	insert_and_print([]byte("18"), t)

	delete_and_test_its_not_there([]byte("10"), t)

	insert_and_print([]byte("45"), t)

	delete_and_test_its_not_there([]byte("18"), t)
}

func delete_with_merge() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("20"), t)
	insert_and_print([]byte("30"), t)
	insert_and_print([]byte("40"), t)
	insert_and_print([]byte("50"), t)

	// Should borrow from left. It also makes
	// the parent empty, so this should replace
	// the parent, which is also the root.
	delete_and_test_its_not_there([]byte("40"), t)

	// Reset
	insert_and_print([]byte("40"), t)

	// Make the tree so that there are more than
	// one items on the parent (so that it doesn't
	// become empty when we take one)
	insert_and_print([]byte("13"), t)
	insert_and_print([]byte("15"), t)
	insert_and_print([]byte("17"), t)

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

	// Insert enough nodes to create parents that are not the
	// root
	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("15"), t)
	insert_and_print([]byte("20"), t)
	insert_and_print([]byte("25"), t)
	insert_and_print([]byte("30"), t)
	insert_and_print([]byte("35"), t)
	insert_and_print([]byte("40"), t)
	insert_and_print([]byte("45"), t)
	insert_and_print([]byte("50"), t)
	insert_and_print([]byte("55"), t)
	insert_and_print([]byte("60"), t)
	insert_and_print([]byte("65"), t)
	insert_and_print([]byte("70"), t)
	insert_and_print([]byte("75"), t)
	insert_and_print([]byte("80"), t)
	insert_and_print([]byte("85"), t)
	insert_and_print([]byte("90"), t)

	delete_and_test_its_not_there([]byte("85"), t)
}

func delete_internal() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("20"), t)
	insert_and_print([]byte("30"), t)
	insert_and_print([]byte("40"), t)
	insert_and_print([]byte("50"), t)

	insert_and_print([]byte("11"), t)
	insert_and_print([]byte("16"), t)
	insert_and_print([]byte("13"), t)
	insert_and_print([]byte("12"), t)

	delete_and_test_its_not_there([]byte("13"), t)

	insert_and_print([]byte("13"), t)

	delete_and_test_its_not_there([]byte("12"), t)
}

func delete_internal_merge() {
	file := "bt.db"
	t := btree.GetNewBTree(2, file)
	defer func() {
		t.Close()
		os.Remove(file)
	}()

	insert_and_print([]byte("10"), t)
	insert_and_print([]byte("20"), t)
	insert_and_print([]byte("30"), t)
	insert_and_print([]byte("40"), t)
	insert_and_print([]byte("50"), t)

	insert_and_print([]byte("11"), t)
	insert_and_print([]byte("16"), t)
	insert_and_print([]byte("13"), t)

	delete_and_test_its_not_there([]byte("13"), t)
}

func main() {
	// deletion1()
	// borrow_from_left()
	// borrow_from_right()
	// delete_with_merge()
	delete_with_merge2()
	// delete_internal()
	// delete_internal_merge()
}
