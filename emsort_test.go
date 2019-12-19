package emsort

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
)

func TestHashSorting(t *testing.T) {
	// Calculate sha256 of concatentation of sorted array of sha256's of "0", "1", ..., "4999999".

	// Control for this value is in file control.py
	want := "faa9d89248e26e9a6441ad4b1ac0543175ee33d20925b861623d0436a5633dbf"

	tmpfile, err := ioutil.TempFile("", "emsort")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	s, err := New(50*1024*1024, tmpfile)
	if err != nil {
		t.Fatal(err)
	}

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
