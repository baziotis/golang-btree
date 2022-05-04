package btree

import (
	"math/rand"
	"os"
	"testing"
	"time"
)

func rand_bytes(n int) bytes_t {
	var letters = bytes_t("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make(bytes_t, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

func TestBTree(t *testing.T) {
	// Remove test .db file
	test_db_file := "bt_test.db"
	if check_file_exists(test_db_file) {
		err := os.Remove("bt_test.db")
		panic_on_err(err)
	}
	// TODO: Check properties of tree, e.g., that all paths
	// to leaves have the same length
	for order := 1; order < 10; order++ {
		bt := GetNewBTree(order, "bt_test.db")
		num_insertions := 3 * 1000
		key_value_len := 10
		rand.Seed(time.Now().UnixNano())

		verifier := make(map[string]string, num_insertions)
		keys_saved := make([]bytes_t, num_insertions)

		// Random insertions
		for i := 0; i < num_insertions; i++ {
			key := rand_bytes(key_value_len)
			value := rand_bytes(key_value_len)
			bt.Insert(key, value)
			found, value_lookup := bt.Find(key)
			if !found || !value.equal(value_lookup) {
				t.Fatal()
			}
			verifier[string(key)] = string(value)
			keys_saved = append(keys_saved, key)
		}

		// Random lookups
		for i := 0; i < 10*num_insertions; i++ {
			// With some probability, either lookup a random
			// key, or one of those saved.
			const prob = 0.7

			get_from_saved := rand.Float32() < prob
			var key bytes_t
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
				if !value_bt.equal(bytes_t(value_map)) {
					t.Fatal()
				}
			}
		}
	}
}
