package main

import (
	"bufio"
	"io"
	"log"
	"os"
)

const (
	minChunk uint64 = 1 << 18
	maxChunk uint64 = 1 << 22
)

type chunkReader struct {
	chunkSize     uint64 // chunSize -- chunk size
	doneChunk     []byte // doneChunk -- copy of readingChunk, interact with file fetcher
	readingChunk  []byte // readingChunk -- readBuffer to read file
	lastRemaining []byte // lastRemaining -- remained bytes following the last recSpliter[0]
	recSpliter    []byte // recSpliter -- identifier(\n or \r) to split contents into records
	attrSpliter   byte   // attrSpliter -- identifer to split record into attributes
}

func NewChunkReader(chunkSize uint64, recSpliter []byte, attrSpliter byte) *chunkReader {
	cr := new(chunkReader)
	cr.chunkSize = chunkSize
	cr.readingChunk = make([]byte, cr.chunkSize, cr.chunkSize)
	cr.doneChunk = make([]byte, 0, cr.chunkSize)
	cr.lastRemaining = make([]byte, 0)
	cr.recSpliter = make([]byte, 0)
	cr.recSpliter = append(cr.recSpliter, recSpliter...)
	cr.attrSpliter = attrSpliter
	return cr
}

// RunReading -- write cr.readingChunk to cr.doneChunk after getting the nextChunk signal.
// Once done, return res to master via chunkEnd channel
func (cr *chunkReader) RunReading(file string, chunkEnd chan bool, nextChunk chan bool) {
	cr.readingChunk = cr.readingChunk[:cap(cr.readingChunk)]
	if fd, err := os.Open(file); err != nil {
		log.Fatalf("[chunReader]-[RunReading] open file %s error\n", file)
		fd.Close()
	} else {
		defer fd.Close()
		readBuf := bufio.NewReaderSize(fd, int(cr.chunkSize)+1)
		for n, err := 0, error(nil); err == nil; {
			n, err = io.ReadFull(readBuf, cr.readingChunk)

			cr.readingChunk = cr.readingChunk[:n]
			if err != nil && err == io.ErrUnexpectedEOF {
				// if the last line of the file doesn't contain '\n', append '\n' to it
				if cr.readingChunk[n-1] != cr.recSpliter[0] {
					cr.readingChunk = append(cr.readingChunk, cr.recSpliter[0])
				}
			}
		Loop:
			for {
				select {
				case <-nextChunk:
					cr.doneChunk = append(cr.doneChunk, cr.readingChunk...)
					if err == io.ErrUnexpectedEOF {
						chunkEnd <- true
					} else {
						chunkEnd <- false
					}
					break Loop
				}
			}

		}
	}
}

// WriteChunk -- write cr.doneChunk to buffer. Buffer always contain complete lines
func (cr *chunkReader) WriteChunk(buffer []byte) (n int) {
	remain := len(cr.lastRemaining)
	for i := 0; i < remain; i++ {
		buffer[i] = cr.lastRemaining[i]
	}
	var lastNewLine int
	for i := 0; i < len(cr.doneChunk); i++ {
		if cr.doneChunk[i] == cr.recSpliter[0] {
			lastNewLine = remain + i
		}
		buffer[remain+i] = cr.doneChunk[i]
	}
	buffer = buffer[:remain+len(cr.doneChunk)]
	n = lastNewLine + 1

	cr.doneChunk = cr.doneChunk[:0]
	cr.lastRemaining = cr.lastRemaining[:0]
	cr.lastRemaining = append(cr.lastRemaining, buffer[lastNewLine+1:]...)
	return
}
