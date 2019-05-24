package main

import (
	"math"
	"runtime"
	"sort"
	"sync"
)

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
