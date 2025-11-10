package cmap

import (
	"bytes"
	"errors"
	"testing"

	"github.com/holmberd/go-cmap/internal/buffer"
)

func TestCMapInsertAndGet(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Clear()

	testCases := []struct {
		name        string
		key         []byte
		val         []byte
		expectedErr error
	}{
		{"Valid entry", []byte("1"), []byte("dag"), nil},
		{"Key too large", make([]byte, buffer.MaxKeySize+1), []byte("dag"), ErrKeyTooLarge},
		{"Value too large", []byte("1"), make([]byte, buffer.MaxValueSize+1), ErrValueTooLarge},
		{"Empty key ", []byte{}, []byte("dag"), ErrKeyEmpty},
		{"Nil key ", []byte{}, []byte("dag"), ErrKeyEmpty},
		{"Empty value", []byte("1"), []byte{}, nil},
		{"Nil value", []byte("1"), nil, nil},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := c.Insert(tc.key, tc.val); err != nil {
				if tc.expectedErr != nil {
					if !errors.Is(err, tc.expectedErr) {
						t.Errorf("expected error %q, got %q", tc.expectedErr, err)
					}
					return
				} else {
					t.Fatalf("failed to insert: %v", err)
				}
			}

			got, ok, err := c.Get(tc.key)
			if err != nil {
				t.Fatalf("failed to get: %v", err)
			}
			if !ok {
				t.Errorf("expected key %q to exist", tc.key)
			}
			if !bytes.Equal(got, tc.val) {
				t.Errorf("expected %q, got %q", tc.val, got)
			}
		})
	}
}

func TestCMapInsertAndUpdate(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Clear()

	key := []byte("1")
	val1 := []byte("v1")
	val2 := []byte("v2")
	updated, err := c.Insert(key, val1)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if updated {
		t.Fatal("expected insert not to update")
	}
	updated, err = c.Insert(key, val2)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if !updated {
		t.Fatal("expected insert to be update")
	}
	got, ok, err := c.Get(key)
	if err != nil || !ok {
		t.Fatalf("failed to get: %v", err)
	}
	if !bytes.Equal(got, val2) {
		t.Errorf("expcted %q, got %q, after update", val2, got)
	}
}

func TestCMapGetMissing(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Clear()

	v, ok, err := c.Get([]byte("\xde\xad\xbe\xef"))
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if ok || v != nil {
		t.Error("expected key to not exist")
	}
}

func TestCMapDelete(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Clear()

	key := []byte("1")
	val := []byte("bye")
	if c.Len() != 0 {
		t.Errorf("expected len to be 0 before insert, got %d", c.Len())
	}
	if _, err = c.Insert(key, val); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if c.Len() != 1 {
		t.Errorf("expected len to be 1 after insert, got %d", c.Len())
	}
	existed, err := c.Delete(key)
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if !existed {
		t.Error("expected key to exist during delete")
	}
	if found := c.Has(key); found {
		t.Error("expected key to not exist after delete")
	}
	if existed, err := c.Delete(key); existed || err != nil {
		t.Error("expected deleting missing key to return false")
	}
	if c.Len() != 0 {
		t.Errorf("expected len to be 0, got %d", c.Len())
	}
}

func TestCMapDeleteMissing(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Clear()

	existed, err := c.Delete([]byte("\xde\xad\xbe\xef"))
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if existed {
		t.Error("expected key to not exist during delete")
	}
}

func TestCMapLen(t *testing.T) {
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Clear()

	if got := c.Len(); got != 0 {
		t.Errorf("expcted len = 0, got %d", got)
	}
	key1 := []byte("1")
	key2 := []byte("2")
	if _, err := c.Insert(key1, []byte("1")); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if _, err := c.Insert(key2, []byte("2")); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	if got := c.Len(); got != 2 {
		t.Errorf("expected len = 2, got %d", got)
	}
	if _, err := c.Delete(key1); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if got := c.Len(); got != 1 {
		t.Errorf("expected len = 1, got %d", got)
	}
	if _, err := c.Delete(key2); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if got := c.Len(); got != 0 {
		t.Errorf("expected len = 0, got %d", got)
	}
}
