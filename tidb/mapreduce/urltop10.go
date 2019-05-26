package main

import (
	"bytes"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
)

func URLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs

	//[Round1]mapPhase: split content into components and do url count ( partial count)
	//[Round1]reducePhase: url count in a whole
	//[Round2]mapPhase: extract top10 URL into []KeyValue
	//[Round2]reducePhase: get top10 from output of all the map workers
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})

	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})

	return args
}

// URLCountMap is the map function in the first round
// URLCountMap split content into components and do url count ( partial count)
func URLCountMap(filename string, contents string) []KeyValue {
	lines := strings.Split(string(contents), "\n")
	mapCounter := make(map[string]int, len(lines)/4+1)

	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		mapCounter[l] += 1
	}

	kvs := make([]KeyValue, 0, len(mapCounter))
	for k, v := range mapCounter {
		kvs = append(kvs, KeyValue{k, strconv.Itoa(v)})
	}

	return kvs
}

// URLCountReduce is the reduce function in the first round
// URLCountReduce collect partial count from map workers and coordinate them into a whole url count
func URLCountReduce(key string, values []string) string {
	sum := 0
	for _, v := range values {
		if cnt, err := strconv.Atoi(v); err != nil {
			log.Fatalln(err)
		} else {
			cnt, _ = strconv.Atoi(v)
			sum += cnt
		}
	}
	res := fmt.Sprintf("%s %s\n", key, strconv.Itoa(sum))
	l := strings.TrimSpace(res)
	tmp := strings.Split(l, " ")
	if len(tmp) < 2 {
		log.Fatalln("R0Reduce crash")
	}
	return fmt.Sprintf("%s %s\n", key, strconv.Itoa(sum))
}

// URLTop10Map is the map function in the second round
// URLTop10Map only returns top10 most frequent URLs of its task
func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")

	// 10 lines ended with "\n" with split into 11 lines, the last line is ""
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	if len(lines) < 10 {
		res := make([]KeyValue, len(lines), len(lines))
		for i, l := range lines {
			res[i] = KeyValue{"", l}
		}
		return res
	}

	top10 := make([]urlCount, 10, 10)

	//initialization
	for i := 0; i < 10; i++ {
		l := lines[i]
		tmp := strings.Split(l, " ")

		if count, err := strconv.Atoi(tmp[1]); err != nil {
			log.Fatalln(err)
		} else {
			top10[i].cnt = count
			top10[i].url = l
		}
	}
	sort.Slice(top10, func(i, j int) bool {
		if top10[i].cnt == top10[j].cnt {
			return top10[i].url > top10[j].url
		}
		return top10[i].cnt < top10[j].cnt
	})

	// find the top10 URL count
	for i := 10; i < len(lines); i++ {
		l := lines[i]
		if len(l) == 0 {
			continue
		}
		tmp := strings.Split(strings.TrimSpace(l), " ")

		if count, err := strconv.Atoi(tmp[1]); err != nil {
			log.Fatalln(err)
		} else {
			if count >= top10[0].cnt {
				if !(count == top10[0].cnt && l >= top10[0].url) {
					top10[0].cnt = count
					top10[0].url = l
					sort.Slice(top10, func(i, j int) bool {
						if top10[i].cnt == top10[j].cnt {
							return top10[i].url > top10[j].url
						}
						return top10[i].cnt < top10[j].cnt
					})
				}

			}
		}
	}

	kvs := make([]KeyValue, 10)
	for i, v := range top10 {
		kvs[i] = KeyValue{"", v.url}
	}

	return kvs
}

// URLTop10Reduce is the reduce function in the second reound
// URLTop10Reduce collect partial top10 urls from map workers to get top10 urls in a whole
func URLTop10Reduce(key string, values []string) string {
	cnts := make(map[string]int, len(values))
	for _, v := range values {
		v := strings.TrimSpace(v)
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		cnts[tmp[0]] = n
	}

	us, cs := TopN(cnts, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}
