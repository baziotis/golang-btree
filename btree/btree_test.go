package btree

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

func rand_bytes(n int) Bytes {
	var letters = Bytes("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make(Bytes, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

// This takes some time.
func TestBTree(t *testing.T) {
	test_db_file := "bt_test.db"
	// Remove test .db file if it exists
	if check_file_exists(test_db_file) {
		err := os.Remove(test_db_file)
		panic_on_err(err)
	}

	defer os.Remove(test_db_file)

	for order := 1; order < 10; order++ {
		bt := GetNewBTree(order, test_db_file)
		_assert(bt.max_items_per_node() < MAX_KEY_VALUES)
		num_insertions := 3 * 1000
		key_value_len := 10
		_assert(key_value_len < MAX_KEY_LEN)
		_assert(key_value_len < MAX_VALUE_LEN)
		rand.Seed(time.Now().UnixNano())

		verifier := make(map[string]string, num_insertions)
		keys_saved := make([]Bytes, num_insertions)

		// Random insertions
		for i := 0; i < num_insertions; i++ {
			key := rand_bytes(key_value_len)
			value := rand_bytes(key_value_len)
			bt.Insert(key, value)
			// Check that it was inserted by searching for it
			found, value_lookup := bt.Find(key)
			if !found || !value.equal(value_lookup) {
				t.Fatal()
			}
			// Make sure we got the correct value
			verifier[string(key)] = string(value)
			keys_saved = append(keys_saved, key)

			// At every insertion, check that the invariants
			// of the B-Tree hold
			visited_root := false
			bt.Traverse(func(n *node_t, tree *BTree) {
				// Skip the root
				if !visited_root {
					return
				}
				if !check_node_invariants(n, tree) {
					t.Fatal("Some node does not have at least min or at most max.")
				}
				visited_root = true
			})

			// With some probability, close the file, and re-open
			// it, to make sure that the b-tree it's persistent.
			const prob = 0.2
			reload_file := rand.Float32() < prob
			if reload_file {
				bt.Close()
				bt = GetNewBTree(order, test_db_file)
			}
		}

		// Random lookups
		for i := 0; i < 10*num_insertions; i++ {
			// With some probability, either lookup a random
			// key, or one of those saved.
			const prob = 0.7

			get_from_saved := rand.Float32() < prob
			var key Bytes
			if get_from_saved {
				key = keys_saved[rand.Intn(len(keys_saved))]
			} else {
				key = rand_bytes(key_value_len)
			}

			found_bt, value_bt := bt.Find(key)
			value_map, found_map := verifier[string(key)]
			if found_bt != found_map {
				t.Fatal()
			}
			if found_bt {
				if !value_bt.equal(Bytes(value_map)) {
					t.Fatal()
				}
			}

			// With some probability, close the file, and re-open
			// it, to make sure that the b-tree it's persistent.
			const prob_to_reload = 0.2
			reload_file := rand.Float32() < prob_to_reload
			if reload_file {
				bt.Close()
				bt = GetNewBTree(order, test_db_file)
			}
		}

		// Remove the file to reset
		_assert(check_file_exists(test_db_file))
		err := os.Remove(test_db_file)
		panic_on_err(err)
	}
}
