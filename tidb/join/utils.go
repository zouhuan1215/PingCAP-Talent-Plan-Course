package main

import (
	"bufio"
	"log"
	"os"
)

const (
	offset64 uint64 = 14695981039346656037
	prime64         = 1099511628211
)

// CSVFileInfo provides size and approximate number of lines of given CSV file
func CSVFileInfo(f0 string) (size int64, nRec int64) {
	fd0, err := os.Open(f0)
	defer fd0.Close()
	if err != nil {
		log.Fatal(err)
	}
	fi0, err := fd0.Stat()
	if err != nil {
		log.Fatal(err)
	}
	buf0 := bufio.NewReader(fd0)
	l0, _, err := buf0.ReadLine()
	if err != nil {
		log.Fatal(err)
	}
	size = fi0.Size()
	nRec = size / int64(len(l0))
	return
}

// fnvHash64 is ported from go library, which is thread-safe.
func fnvHash64(data []byte) uint64 {
	hash := offset64
	for _, c := range data {
		hash *= prime64
		hash ^= uint64(c)
	}
	return hash
}
