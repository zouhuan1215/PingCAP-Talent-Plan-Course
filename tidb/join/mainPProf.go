package main

import (
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

var cpu = flag.String("cpu", "", "write cpu profile to file")
var mem = flag.String("mem", "", "write mem profile to file")
var interval = flag.String("interval", "100", "intervals for goroutine counter")
var t0 = flag.String("t0", "0", "relation0")
var t1 = flag.String("t1", "1", "relation1")
var off0 = flag.String("off0", "0", "joinAttributes")
var off1 = flag.String("off1", "0", "joinAttributes")

func main() {
	flag.Parse()
	// ==================== cpu ======================
	if *cpu != "" {
		f, err := os.Create(*cpu)
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
	if *interval != "" {
		log.SetFlags(log.Ltime | log.LUTC)
		log.SetOutput(os.Stdout)

		go func() {
			goroutines := pprof.Lookup("goroutine")
			if seconds, err := strconv.Atoi(*interval); err != nil {
				log.Fatalln(err)
			} else {
				for range time.Tick(time.Duration(seconds) * time.Second) {
					log.Printf("goroutine count: %d\n", goroutines.Count())
				}
			}

		}()
	}

	// ======== running code here ============
	//	f0 := "./t/r0.tbl"
	//	f1 := "./t/r0.tbl"
	//	offsets0 := []int{0}
	//	offsets1 := []int{1}
	var f0, f1 string
	offsets0 := make([]int, 0)
	offsets1 := make([]int, 0)
	if *t0 != "" {
		f0 = *t0
	}
	if *t1 != "" {
		f1 = *t1
	}
	if *off0 != "" {
		vals := strings.Split(*off0, ",")
		for _, val := range vals {
			if v, err := strconv.Atoi(val); err != nil {
				log.Fatalln(err)
			} else {
				offsets0 = append(offsets0, v)
			}
		}
	}

	if *off1 != "" {
		vals := strings.Split(*off1, ",")
		for _, val := range vals {
			if v, err := strconv.Atoi(val); err != nil {
				log.Fatalln(err)
			} else {
				offsets1 = append(offsets1, v)
			}
		}
	}
	
	Join(f0, f1, offsets0, offsets1)

	// =========== MemProfile ============
	if *mem != "" {
		f, err := os.Create(*mem)
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
