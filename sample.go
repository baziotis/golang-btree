package main

import (
	"os"

	"github.com/baziotis/golang-btree/btree"
)

func main() {
	file := "bt.db"
	t := btree.GetNewBTree(1, file)
	defer os.Remove(file)

	t.Insert([]byte("10"), []byte("24"))
	t.Print()
	t.Insert([]byte("20"), []byte("38"))
	t.Print()
	t.Insert([]byte("30"), []byte("457"))
	t.Print()
	t.Insert([]byte("40"), []byte("7"))
	t.Print()
	t.Insert([]byte("50"), []byte("24"))
	t.Print()
	t.Insert([]byte("60"), []byte("32"))
	t.Print()
	t.Insert([]byte("70"), []byte("7"))
	t.Print()
	t.Insert([]byte("80"), []byte("7"))
	t.Print()
	t.Insert([]byte("0"), []byte("7"))
	t.Print()
	t.Insert([]byte("05"), []byte("7"))
	t.Print()
	t.Insert([]byte("35"), []byte("7"))
	t.Print()
	t.Insert([]byte("36"), []byte("7"))
	t.Print()

	t.Close()
}
