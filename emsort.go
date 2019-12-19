// Package emsort provides a facility for performing disk-based external
// merge sorts.
//
// see https://en.wikipedia.org/wiki/External_sorting#External_merge_sort
// see http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
package emsort

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"os"
	"sort"
)

func New(memLimit int, tmpfile *os.File) (*ExternalSorter, error) {
	return &ExternalSorter{
		tmpfile:  tmpfile,
		memLimit: memLimit,
	}, nil
}

type ExternalSorter struct {
	tmpfile  *os.File
	memLimit int
	memUsed  int
	sizes    []int
	records  [][]int
	vals     [][]byte

	// Reading.
	entries *entryHeap
}

func (s *ExternalSorter) Push(b []byte) error {
	s.vals = append(s.vals, b)
	s.memUsed += len(b)
	if s.memUsed >= s.memLimit {
		if err := s.flush(); err != nil {
			return err
		}
	}
	return nil
}

func (s *ExternalSorter) flush() error {
	sort.Sort(&inmemory{s.vals})

	out := bufio.NewWriterSize(s.tmpfile, 16*1024*1024)
	for _, val := range s.vals {
		if _, err := out.Write(val); err != nil {
			return err
		}
	}
	if err := out.Flush(); err != nil {
		return err
	}

	size := 0
	records := make([]int, 0, len(s.vals))
	for _, val := range s.vals {
		size += len(val)
		records = append(records, len(val))
	}
	s.sizes = append(s.sizes, size)
	s.records = append(s.records, records)
	s.vals = s.vals[:0]
	s.memUsed = 0

	return nil
}

func (s *ExternalSorter) StopWriting() error {
	if s.memUsed > 0 {
		if err := s.flush(); err != nil {
			return err
		}
	}

	// Free memory used by last read vals
	s.vals = nil

	files := make([]*bufio.Reader, len(s.sizes))
	total := 0
	for i, size := range s.sizes {
		file := io.NewSectionReader(s.tmpfile, int64(total), int64(size))
		total += size
		files[i] = bufio.NewReaderSize(file, s.memLimit/len(s.sizes))
	}

	s.entries = &entryHeap{
		entries: make([]*entry, len(files)),
	}
	for i, file := range files {
		e := &entry{
			file:    file,
			records: s.records[i],
		}
		has, err := e.Read()
		if err != nil {
			return err
		}
		if !has {
			return fmt.Errorf("Unexpected empty file")
		}
		s.entries.entries[i] = e
	}
	heap.Init(s.entries)

	return nil
}

func (s *ExternalSorter) Pop() (result []byte, err error) {
	if s.entries.Len() == 0 {
		return nil, io.EOF
	}

	e := heap.Pop(s.entries).(*entry)
	result = e.val

	has, err := e.Read()
	if err != nil {
		return nil, err
	}
	if has {
		heap.Push(s.entries, e)
	}

	return
}

type inmemory struct {
	vals [][]byte
}

func (im *inmemory) Len() int {
	return len(im.vals)
}

func (im *inmemory) Less(i, j int) bool {
	return bytes.Compare(im.vals[i], im.vals[j]) == -1
}

func (im *inmemory) Swap(i, j int) {
	im.vals[i], im.vals[j] = im.vals[j], im.vals[i]
}

type entry struct {
	file    io.Reader
	val     []byte
	records []int
}

func (e *entry) Read() (bool, error) {
	if len(e.records) == 0 {
		return false, nil
	}

	size := e.records[0]
	e.records = e.records[1:]

	e.val = make([]byte, size)

	if _, err := io.ReadFull(e.file, e.val); err != nil {
		return false, err
	}
	return true, nil
}

type entryHeap struct {
	entries []*entry
}

func (eh *entryHeap) Len() int {
	return len(eh.entries)
}

func (eh *entryHeap) Less(i, j int) bool {
	return bytes.Compare(eh.entries[i].val, eh.entries[j].val) == -1
}

func (eh *entryHeap) Swap(i, j int) {
	eh.entries[i], eh.entries[j] = eh.entries[j], eh.entries[i]
}

func (eh *entryHeap) Push(x interface{}) {
	eh.entries = append(eh.entries, x.(*entry))
}

func (eh *entryHeap) Pop() interface{} {
	n := len(eh.entries)
	x := eh.entries[n-1]
	eh.entries = eh.entries[:n-1]
	return x
}
