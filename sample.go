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
	t.Print(print_values)
	found, _ := t.Find(key)
	_assert(!found)
	t.Print(print_values)
}

func main() {
	// example1()
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
