package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// ReduceF function from MIT 6.824 LAB1
type ReduceF func(key string, values []string) string

// MapF function from MIT 6.824 LAB1
type MapF func(filename string, contents string) []KeyValue

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

type task struct {
	dataDir    string
	jobName    string
	mapFile    string
	phase      jobPhase
	taskNumber int
	nMap       int
	nReduce    int
	mapF       MapF
	reduceF    ReduceF
	wg         sync.WaitGroup
}

// MRCluster represents a map-reduce cluster.
type MRCluster struct {
	nWorkers int
	wg       sync.WaitGroup
	taskCh   chan *task
	exit     chan struct{}
}

var singleton = &MRCluster{
	nWorkers: runtime.NumCPU(),
	taskCh:   make(chan *task),
	exit:     make(chan struct{}),
}

func init() {
	singleton.Start()
}

func GetMRCluster() *MRCluster {
	return singleton
}

func (c *MRCluster) NWorkers() int { return c.nWorkers }

func (c *MRCluster) Start() {
	for i := 0; i < c.nWorkers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
}

func (c *MRCluster) worker() {
	defer c.wg.Done()
	content := make([]byte, 0)
	reduceOut := make([]byte, 0)
	for {
		select {
		case t := <-c.taskCh:
			if t.phase == mapPhase {
				fm, err := os.Open(t.mapFile)
				if err != nil {
					panic(err)
				}
				bm := bufio.NewReader(fm)

				fi, err := fm.Stat()
				if err != nil {
					panic(err)
				}
				size := int(fi.Size())
				if size > len(content) {
					content = append(content, make([]byte, size-len(content), size-len(content))...)
				} else {
					content = content[:size]
				}

				bm.Read(content)

				fm.Close()

				fs := make([]*os.File, t.nReduce)
				bs := make([]*bufio.Writer, t.nReduce)
				for i := range fs {
					rpath := reduceName(t.dataDir, t.jobName, t.taskNumber, i)
					fs[i], bs[i] = CreateFileAndBuf(rpath)
				}
				results := t.mapF(t.mapFile, String(content))

				var k string
				var v string
				for _, kv := range results {
					k = strings.Trim(kv.Key, ",")
					v = strings.Trim(kv.Value, ",")

					bs[fnvHash64([]byte(kv.Key))%t.nReduce].WriteString(fmt.Sprintf("%v,%v\n", k, v))
				}
				for i := range fs {
					SafeClose(fs[i], bs[i])
				}
			} else {
				// interData -- the input of reduceF
				interData := make(map[string][]string)
				for i := 0; i < t.nMap; i++ {
					reduceInfile := reduceName(t.dataDir, t.jobName, i, t.taskNumber)
					fs, err := os.Open(reduceInfile)
					if err != nil {
						panic(err)
					}
					bs := bufio.NewReader(fs)

					// read the whole map file into content
					fi, err := fs.Stat()
					if err != nil {
						panic(err)
					}
					size := int(fi.Size())
					if size > len(content) {
						content = append(content, make([]byte, size-len(content), size-len(content))...)
					} else {
						content = content[:size]
					}
					bs.Read(content)

					// parse content into key-values and append values with the same key to interdata[key]
					start := 0
					comma := 0
					var k string
					var v string
					for i := 0; i < len(content); i++ {
						if content[i] == 0x2c {
							comma = i
							if start == comma {
								k = ""
							} else {
								k = string(content[start:comma])
							}
						}
						if content[i] == 0x0a {
							v = string(content[comma+1 : i])
							start = i + 1
							interData[k] = append(interData[k], v)
						}

					}
					fs.Close()
				}

				//reduceOut -- the res of reducePhase
				reduceOut = reduceOut[:0]
				for k, v := range interData {
					reduceOut = append(reduceOut, []byte(t.reduceF(k, v))...)
				}

				// write res to disk
				reduceOutFile := mergeName(t.dataDir, t.jobName, t.taskNumber)
				fs, bs := CreateFileAndBuf(reduceOutFile)
				if _, err := bs.Write(reduceOut); err != nil {
					log.Fatalln(err)
				}
				SafeClose(fs, bs)

			}

			t.wg.Done()
		case <-c.exit:
			return
		}
	}
}

func (c *MRCluster) Shutdown() {
	close(c.exit)
	c.wg.Wait()
}

func (c *MRCluster) Submit(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int) <-chan []string {
	notify := make(chan []string)
	go c.run(jobName, dataDir, mapF, reduceF, mapFiles, nReduce, notify)
	return notify
}

func (c *MRCluster) run(jobName, dataDir string, mapF MapF, reduceF ReduceF, mapFiles []string, nReduce int, notify chan<- []string) {
	nMap := len(mapFiles)
	tasks := make([]*task, 0, nMap)

	for i := 0; i < nMap; i++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			mapFile:    mapFiles[i],
			phase:      mapPhase,
			taskNumber: i,
			nReduce:    nReduce,
			nMap:       nMap,
			mapF:       mapF,
		}
		t.wg.Add(1)
		tasks = append(tasks, t)
		go func() { c.taskCh <- t }()
	}
	for _, t := range tasks {
		t.wg.Wait()
	}

	reduceTasks := make([]*task, 0, nReduce)
	for i := 0; i < nReduce; i++ {
		t := &task{
			dataDir:    dataDir,
			jobName:    jobName,
			phase:      reducePhase,
			taskNumber: i,
			nReduce:    nReduce,
			nMap:       nMap,
			reduceF:    reduceF,
		}
		t.wg.Add(1)
		reduceTasks = append(reduceTasks, t)
		go func() { c.taskCh <- t }()
	}
	for _, t := range reduceTasks {
		t.wg.Wait()
	}

	// reduce phase done, notify the position of the output files
	outputs := make([]string, 0)
	for i := 0; i < nReduce; i++ {
		outputs = append(outputs, mergeName(dataDir, jobName, i))
	}

	go func() { notify <- outputs }()
	return
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func reduceName(dataDir, jobName string, mapTask int, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-"+strconv.Itoa(mapTask)+"-"+strconv.Itoa(reduceTask))
}

func mergeName(dataDir, jobName string, reduceTask int) string {
	return path.Join(dataDir, "mrtmp."+jobName+"-res-"+strconv.Itoa(reduceTask))
}
