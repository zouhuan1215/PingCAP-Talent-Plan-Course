package main

import (
	"flag"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"time"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")
var num = flag.String("num", "8", "number of nodes")

func ffprepare(src []int64) {
	rand.Seed(time.Now().Unix())
	for i := range src {
		src[i] = rand.Int63()
	}
}

func main() {
	flag.Parse()
	// ==================== CPUProfile ======================
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// =========== count number of goroutine running every 100s ===============
	if *num != "" {
		_, err := strconv.Atoi(*num)
		if err != nil {
			log.Fatal(err)
		}
		// nCPU := n
	}

	// ======== running code here ============
	lens := []int{1, 3, 5, 7, 11, 13, 17, 19, 23, 29, 1024, 1 << 13, 1 << 17, 1 << 19, 1 << 20}

	for i := range lens {
		src := make([]int64, lens[i])
		expect := make([]int64, lens[i])
		ffprepare(src)
		copy(expect, src)
		MergeSort(src)
	}

	// =========== MemProfile ============
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

}

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the home work.
func MergeSort(src []int64) {
	nCPU := runtime.NumCPU()
	nNodes := nCPU * 8
	if nNodes > 64 {
		nNodes = 64
	}
	height := int(math.Log2(float64(nNodes)))
	mergeSort(height, src)
}

func mergeSort(height int, src []int64) {
	wg := sync.WaitGroup{}
	wg.Add(1)

	s := make([]int64, len(src), len(src))
	go mergesort(height, 0, src, s, &wg)
	wg.Wait()
}

func mergesort(height int, myHeight int, src []int64, slice []int64, wg *sync.WaitGroup) {
	if height-myHeight > 0 {
		nxt := myHeight + 1

		childWg := sync.WaitGroup{}
		childWg.Add(2)

		// send right half data to rightChild
		go mergesort(height, nxt, src[(len(src))/2:], slice[(len(src))/2:], &childWg)
		// send left half data to leftChild
		mergesort(height, nxt, src[:(len(src))/2], slice[:(len(src))/2], &childWg)

		// wait until children nodes have sorted the data chunk
		childWg.Wait()
		if myHeight%2 != 0 {
			sortedRight := src[(len(src))/2:]
			sortedLeft := src[:(len(src))/2]

			// merge sortedLeft and sortedRight
			size, i, j := len(sortedLeft)+len(sortedRight), 0, 0
			for k := 0; k < size; k++ {
				if i > len(sortedLeft)-1 && j <= len(sortedRight)-1 {
					slice[k] = sortedRight[j]
					j++
				} else if i <= len(sortedLeft)-1 && j > len(sortedRight)-1 {
					slice[k] = sortedLeft[i]
					i++
				} else if sortedLeft[i] <= sortedRight[j] {
					slice[k] = sortedLeft[i]
					i++
				} else {
					slice[k] = sortedRight[j]
					j++
				}
			}
		} else {
			sortedRight := slice[(len(src))/2:]
			sortedLeft := slice[:(len(src))/2]

			// merge sortedLeft and sortedRight
			size, i, j := len(sortedLeft)+len(sortedRight), 0, 0
			for k := 0; k < size; k++ {
				if i > len(sortedLeft)-1 && j <= len(sortedRight)-1 {
					src[k] = sortedRight[j]
					j++
				} else if i <= len(sortedLeft)-1 && j > len(sortedRight)-1 {
					src[k] = sortedLeft[i]
					i++
				} else if sortedLeft[i] <= sortedRight[j] {
					src[k] = sortedLeft[i]
					i++
				} else {
					src[k] = sortedRight[j]
					j++
				}
			}
		}
	} else {
		sort.Slice(src, func(i, j int) bool { return src[i] < src[j] })
		if myHeight%2 != 0 {
			copy(slice, src)
		}
	}
	wg.Done()
}
