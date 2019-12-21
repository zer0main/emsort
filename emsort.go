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
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

const writeBufferSize = 16 * 1024 * 1024

type File interface {
	io.Writer
	io.ReaderAt
}

func New(memLimit int, tmpfile File) (*ExternalSorter, error) {
	return &ExternalSorter{
		tmpfile:  tmpfile,
		memLimit: memLimit,
	}, nil
}

func NewFixedSize(recordSize, memLimit int, tmpfile File) (*ExternalSorter, error) {
	return &ExternalSorter{
		tmpfile:    tmpfile,
		recordSize: recordSize,
		memLimit:   memLimit,
	}, nil
}

type ExternalSorter struct {
	tmpfile    File
	recordSize int
	memLimit   int
	memUsed    int
	sizes      []int
	vals       [][]byte

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

	out := bufio.NewWriterSize(s.tmpfile, writeBufferSize)
	sizeBuf := make([]byte, binary.MaxVarintLen64)
	size := 0
	for _, val := range s.vals {
		if s.recordSize == 0 {
			n := binary.PutUvarint(sizeBuf, uint64(len(val)))
			if _, err := out.Write(sizeBuf[:n]); err != nil {
				return err
			}
			size += n
		}
		if _, err := out.Write(val); err != nil {
			return err
		}
		size += len(val)
	}
	if err := out.Flush(); err != nil {
		return err
	}

	s.sizes = append(s.sizes, size)
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
			file:       file,
			recordSize: s.recordSize,
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
	file       *bufio.Reader
	val        []byte
	recordSize int
}

func (e *entry) Read() (bool, error) {
	size := e.recordSize
	if size == 0 {
		size64, err := binary.ReadUvarint(e.file)
		if err == io.EOF {
			return false, nil
		} else if err != nil {
			return false, err
		}
		size = int(size64)
	}

	e.val = make([]byte, size)

	if _, err := io.ReadFull(e.file, e.val); err != nil {
		if err == io.EOF && e.recordSize != 0 {
			err = nil
		}
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
