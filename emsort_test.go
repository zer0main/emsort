package emsort

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type sorter interface {
	Push(b []byte) error
	StopWriting() error
	Pop() ([]byte, error)
}

func checkHashes(t *testing.T, s sorter) {
	// Calculate sha256 of concatentation of sorted array of sha256's of "0", "1", ..., "4999999".

	t.Parallel()

	// Control for this value is in file control.py
	want := "faa9d89248e26e9a6441ad4b1ac0543175ee33d20925b861623d0436a5633dbf"

	for i := 0; i < 5000000; i++ {
		text := strconv.Itoa(i)
		hash := sha256.Sum256([]byte(text))
		if err := s.Push(hash[:]); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.StopWriting(); err != nil {
		t.Fatal(err)
	}

	hasher := sha256.New()
	for {
		record, err := s.Pop()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if _, err := hasher.Write(record); err != nil {
			panic(err)
		}
	}
	sum := hasher.Sum(nil)

	got := hex.EncodeToString(sum)

	if got != want {
		t.Errorf("Got %s, want %s.", got, want)
	}
}

func TestHashSorting(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "emsort")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	s, err := New(50*1024*1024, tmpfile)
	if err != nil {
		t.Fatal(err)
	}

	checkHashes(t, s)
}

func TestHashSortingFixed(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "emsort")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	s, err := NewFixedSize(sha256.Size, 50*1024*1024, tmpfile)
	if err != nil {
		t.Fatal(err)
	}

	checkHashes(t, s)
}

func TestHashSortingLevel(t *testing.T) {
	dir, err := ioutil.TempDir("", "emsort-level")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := &leveldbSorter{db: db}
	defer func() {
		s.iter.Release()
	}()

	checkHashes(t, s)
}

type leveldbSorter struct {
	db   *leveldb.DB
	iter iterator.Iterator
}

var value = []byte("")

func (s *leveldbSorter) Push(b []byte) error {
	return s.db.Put(b, value, nil)
}

func (s *leveldbSorter) StopWriting() error {
	s.iter = s.db.NewIterator(nil, nil)
	return nil
}

func (s *leveldbSorter) Pop() ([]byte, error) {
	if !s.iter.Next() {
		return nil, io.EOF
	}
	return s.iter.Key(), nil
}
