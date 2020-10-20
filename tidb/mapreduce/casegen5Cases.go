package main

import (
	"math/rand"
	"time"
)

// this file is for the convenience of test
// select only 5 genFunc out of 10 randomly

func generateRandomNumber(start int, end int, count int) []int {

	if end < start || (end-start) < count {
		return nil
	}

	nums := make([]int, 0)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(nums) < count {
		num := r.Intn((end - start)) + start

		exist := false
		for _, v := range nums {
			if v == num {
				exist = true
				break
			}
		}

		if !exist {
			nums = append(nums, num)
		}
	}

	return nums
}
