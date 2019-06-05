package main

import (
	"math"
	"runtime"
)

const (
	minMapPreAlloc uint = 1 << 8
	maxMapPreAlloc uint = 1 << 10
)

type hashJoin struct {
	f0         string // f0 -- input file for relation r0
	off0       []int  // offsets0 -- joined attributes of relation r0
	f1         string // f1 -- input file for relation r1
	off1       []int  // offsets1 -- joined attributes of relation r1
	innerTable bool   // innerTable -- true: build hashtable on r0, false: build hashtable on r1

	cr          *chunkReader      // chunkReader -- responsible for reading file chunk
	chunkSize   uint64            // chunSize -- chunk size
	hashtable   map[uint64]uint64 // hashtable -- output map of build-phase of hash join
	mapPreAlloc uint              // mapPreAlloc -- Pre-Alloc space size for hashtable
	nWorker     int               // nWorker -- number of workers of probe-phase of hash join
	taskChunks  [][]byte          // taskBuffers -- buffer task data for workers
	freeBuf     chan int          // freeBuf -- available chunkBuf
	taskCh      chan task         // taskCh -- hand out available task chunks to worker
	resCh       chan uint64       // resCh -- workers' output for task chunks
}

type task struct {
	nBytes int // nBytes -- how many bytes are available in buf
	bufID  int // bufID -- which buf to read
}

func Join(f0, f1 string, offset0, offset1 []int) (sum uint64) {
        hj := NewHashJoin(f0, f1, offset0, offset1)
        return hj.Join()
}

func NewHashJoin(f0, f1 string, off0, off1 []int) (hj *hashJoin) {
	hj = new(hashJoin)
	hj.f0 = f0
	hj.f1 = f1
	hj.off0 = make([]int, len(off0), len(off0))
	copy(hj.off0, off0)
	hj.off1 = make([]int, len(off1), len(off1))
	copy(hj.off1, off1)
	hj.nWorker = runtime.NumCPU()
	hj.init(f0, f1)
	return
}

// init decide which relation is used to build hashtable
// decide the chunk size, initiate chunkReader
// decide the mapPreAlloc size
func (hj *hashJoin) init(f0, f1 string) {
	size0, nRec0 := CSVFileInfo(f0)
	size1, nRec1 := CSVFileInfo(f1)

	if size0 > size1 {
		hj.chunkSize = uint64(size0 / int64(hj.nWorker))
	} else {
		hj.chunkSize = uint64(size1 / int64(hj.nWorker))
	}
	if hj.chunkSize < minChunk {
		hj.chunkSize = minChunk
	}
	if hj.chunkSize > maxChunk {
		hj.chunkSize = maxChunk
	}

	var recSpliter []byte
	switch runtime.GOOS {
	case "darwin":
		recSpliter = []byte{0x0a}
	case "windows":
		recSpliter = []byte{0x0a, 0x0d}
	case "linux":
		recSpliter = []byte{0x0a}
	}
	var attrSpliter byte = 0x2c

	hj.cr = NewChunkReader(hj.chunkSize, recSpliter, attrSpliter)

	if nRec0 <= int64(1.2*float64(nRec1)) {
		// build hashtable on r0
		hj.innerTable = true
		hj.mapPreAlloc = 1 << uint(math.Floor(math.Log2(float64(nRec0/2))))
	} else {
		// build hashtable on r1
		hj.innerTable = false
		hj.mapPreAlloc = 1 << uint(math.Floor(math.Log2(float64(nRec1/2))))
	}

	if hj.mapPreAlloc < minMapPreAlloc {
		hj.mapPreAlloc = minMapPreAlloc
	}
	if hj.mapPreAlloc > maxMapPreAlloc {
		hj.mapPreAlloc = maxMapPreAlloc
	}
}

func (hj *hashJoin) Join() (sum uint64) {
	hj.BuildHashtable()
	return hj.Probe()
}

func (hj *hashJoin) BuildHashtable() {
	hj.hashtable = make(map[uint64]uint64, hj.mapPreAlloc)
	if hj.innerTable == true {
		hj.buildHashtable0()
	} else {
		hj.buildHashtable1()
	}
}

func (hj *hashJoin) Probe() (sum uint64) {
	hj.taskCh = make(chan task)
	hj.resCh = make(chan uint64)
	hj.freeBuf = make(chan int)
	cr := hj.cr
	exit := make(chan struct{})
	chunkReady := make(chan bool)
	nextChunk := make(chan bool)
	defer close(chunkReady)
	defer close(nextChunk)
	defer close(exit)

	if hj.innerTable == true {
		go cr.RunReading(hj.f1, chunkReady, nextChunk)
		go func() { nextChunk <- true }()
		for i := 0; i < hj.nWorker; i++ {
			go hj.probeWorker0(exit)
		}
	} else {
		go cr.RunReading(hj.f0, chunkReady, nextChunk)
		go func() { nextChunk <- true }()
		for i := 0; i < hj.nWorker; i++ {
			go hj.probeWorker1(exit)
		}
	}

	hj.taskChunks = [][]byte{make([]byte, cr.chunkSize+1<<10, cr.chunkSize+1<<10)}
	go func() { hj.freeBuf <- 0 }()
	var t task
	var nRead int
	var nRes int
	var eof bool = false
	for {
		select {
		case eof = <-chunkReady:
			select {
			case t.bufID = <-hj.freeBuf:
			default:
				hj.taskChunks = append(hj.taskChunks, make([]byte, cr.chunkSize+1<<10, cr.chunkSize+1<<10))
				t.bufID = len(hj.taskChunks) - 1
			}
			t.nBytes = cr.WriteChunk(hj.taskChunks[t.bufID])
			nRead++
			if !eof {
				go func() { nextChunk <- true }()
			}
			go func(t task) { hj.taskCh <- t }(t)
		case s := <-hj.resCh:
			nRes++
			sum += s
		}
		if eof && (nRead == nRes) {
			return
		}
	}
}

func (hj *hashJoin) buildHashtable0() {
	cr := hj.cr
	chunkReady := make(chan bool)
	nextChunk := make(chan bool)
	defer close(chunkReady)
	defer close(nextChunk)
	go cr.RunReading(hj.f0, chunkReady, nextChunk)
	nextChunk <- true
	records := make([]byte, cr.chunkSize+1<<10, cr.chunkSize+1<<10)
	var nBytes int
	attrShift := make([]int, 0)

	var keyBuffer []byte
	var hashkey uint64
	var val uint64
	var eof bool = false
	for {
		select {
		case eof = <-chunkReady:
			nBytes = cr.WriteChunk(records)
			if !eof {
				go func() { nextChunk <- true }()
			}

			attrShift = attrShift[:0]
			attrShift = append(attrShift, -1)
			for i := 0; i < nBytes; i++ {
				if records[i] == cr.attrSpliter {
					attrShift = append(attrShift, i)
				}
				if records[i] == cr.recSpliter[0] {
					if len(attrShift) >= 2 {
						attrShift = append(attrShift, i)
						for _, off := range hj.off0 {
							keyBuffer = append(keyBuffer, records[attrShift[off]+1:attrShift[off+1]]...)
						}

						for i := 0; i < len(cr.recSpliter); i++ {
							for ind, val := range keyBuffer {
								if val == cr.recSpliter[i] {
									keyBuffer = append(keyBuffer[:ind], keyBuffer[ind+1:]...)
								}
							}
						}

						hashkey = fnvHash64(keyBuffer)
						// parse attr[0] into uint64
						for i := attrShift[0] + 1; i < attrShift[1]; i++ {
							val = val*10 + (uint64(records[i]) - 48)
						}
						hj.hashtable[hashkey] += val

						attrShift = attrShift[:0]
						keyBuffer = keyBuffer[:0]
						val = 0
						attrShift = append(attrShift, i)
					}
				}
			}
		}
		if eof {
			break
		}
	}
	return

}

func (hj *hashJoin) probeWorker0(exit chan struct{}) {
	var t task
	var records []byte
	var hashkey uint64
	attrShift := make([]int, 0)
	keyBuffer := make([]byte, 0)
	for {
		select {
		case t = <-hj.taskCh:
			var res uint64
			records = hj.taskChunks[t.bufID]

			// parse the dataChunk into attributes and handle it
			attrShift = attrShift[:0]
			attrShift = append(attrShift, -1)
			for i := 0; i < t.nBytes; i++ {
				if records[i] == hj.cr.attrSpliter {
					attrShift = append(attrShift, i)
				}
				if records[i] == hj.cr.recSpliter[0] {
					if len(attrShift) >= 2 {
						attrShift = append(attrShift, i)
						for _, off := range hj.off1 {
							keyBuffer = append(keyBuffer, records[attrShift[off]+1:attrShift[off+1]]...)
						}

						for i := 0; i < len(hj.cr.recSpliter); i++ {
							for ind, val := range keyBuffer {
								if val == hj.cr.recSpliter[i] {
									keyBuffer = append(keyBuffer[:ind], keyBuffer[ind+1:]...)
								}
							}
						}
						hashkey = fnvHash64(keyBuffer)
						if val, ok := hj.hashtable[hashkey]; ok {
							res += val
						}
						attrShift = attrShift[:0]
						keyBuffer = keyBuffer[:0]
						attrShift = append(attrShift, i)
					}
				}
			}
			go func(res uint64) { hj.resCh <- res }(res)
			go func(bufID int) { hj.freeBuf <- bufID }(t.bufID)
		case <-exit:
			return
		}
	}
}

func (hj *hashJoin) buildHashtable1() {
	cr := hj.cr
	chunkReady := make(chan bool)
	nextChunk := make(chan bool)
	defer close(chunkReady)
	defer close(nextChunk)
	go cr.RunReading(hj.f1, chunkReady, nextChunk)
	nextChunk <- true
	records := make([]byte, cr.chunkSize+1<<10, cr.chunkSize+1<<10)
	var nBytes int
	attrShift := make([]int, 0)

	var keyBuffer []byte
	var hashkey uint64
	var eof bool = false
	for {
		select {
		case eof = <-chunkReady:
			nBytes = cr.WriteChunk(records)
			if !eof {
				go func() { nextChunk <- true }()
			}

			attrShift = attrShift[:0]
			attrShift = append(attrShift, -1)
			for i := 0; i < nBytes; i++ {
				if records[i] == cr.attrSpliter {
					attrShift = append(attrShift, i)
				}
				if records[i] == cr.recSpliter[0] {
					if len(attrShift) >= 2 {
						attrShift = append(attrShift, i)
						for _, off := range hj.off1 {
							keyBuffer = append(keyBuffer, records[attrShift[off]+1:attrShift[off+1]]...)
						}
						for i := 0; i < len(cr.recSpliter); i++ {
							for ind, val := range keyBuffer {
								if val == cr.recSpliter[i] {
									keyBuffer = append(keyBuffer[:ind], keyBuffer[ind+1:]...)
								}
							}
						}
						hashkey = fnvHash64(keyBuffer)
						// store the count
						hj.hashtable[hashkey]++

						attrShift = attrShift[:0]
						keyBuffer = keyBuffer[:0]
						attrShift = append(attrShift, i)
					}
				}
			}
		}
		if eof {
			break
		}
	}
	return
}

func (hj *hashJoin) probeWorker1(exit chan struct{}) {
	var t task
	var records []byte
	var hashkey uint64
	var val uint64
	attrShift := make([]int, 0)
	keyBuffer := make([]byte, 0)
	for {
		select {
		case t = <-hj.taskCh:
			var res uint64
			records = hj.taskChunks[t.bufID]
			attrShift = attrShift[:0]
			attrShift = append(attrShift, -1)
			for i := 0; i < t.nBytes; i++ {
				if records[i] == hj.cr.attrSpliter {
					attrShift = append(attrShift, i)
				}
				if records[i] == hj.cr.recSpliter[0] {
					if len(attrShift) >= 2 {
						attrShift = append(attrShift, i)
						for _, off := range hj.off0 {
							keyBuffer = append(keyBuffer, records[attrShift[off]+1:attrShift[off+1]]...)
						}
						for i := 0; i < len(hj.cr.recSpliter); i++ {
							for ind, val := range keyBuffer {
								if val == hj.cr.recSpliter[i] {
									keyBuffer = append(keyBuffer[:ind], keyBuffer[ind+1:]...)
								}
							}
						}
						// parse attr[0] into uint64
						for i := attrShift[0] + 1; i < attrShift[1]; i++ {
							val = val*10 + (uint64(records[i]) - 48)
						}
						hashkey = fnvHash64(keyBuffer)
						if count, ok := hj.hashtable[hashkey]; ok {
							res += val * count
						}

						attrShift = attrShift[:0]
						keyBuffer = keyBuffer[:0]
						val = 0
						attrShift = append(attrShift, i)
					}
				}
			}
			go func(res uint64) { hj.resCh <- res }(res)
			go func(bufID int) { hj.freeBuf <- bufID }(t.bufID)
		case <-exit:
			return
		}
	}
}
