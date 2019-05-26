# 1. Ideas
基本思路如下图所示。
<div align="center">
<img src="./img/MRWorker.png" height=300 width=400>
</div>  

URLTop10的统计分为两轮进行:
- > Round 1: 统计每个URL出现的次数
    
    &nbsp;**MapPhase:** 
    1. `master`将input data分成**nMap**个dataChunk, 然后形成**nMap**个task分发给worker
    2. 每个`worker`收到task后，分别统计自己负责的dataChunk中每个URL出现的次数，将结果写入KeyValue，其中Key为URL，Value为对应的URL在该dataChunk中出现的次数
    3. 每个`worker`根据Key进行分类，把KeyValue写到相应的文件中(共**nReduce**个文件)

     &nbsp;**ReducePhase:** 
     1. `master`将MapPhase中**nMap**个worker输出的**nMap\*nReduce**个文件形成**nReduce**个task分发给worker
     2. 每个`worker`收到task后，将**nMap**个文件中，相同Key下的所有Value拣到一起，相加，得到同一个URL在所有dataChunks中的count
     3. 每个`worker`统计完自己负责的部分URL的Global Count后，将结果写入一个文件中

- > Round 2: 根据每个URL的Count，算出10 most frequent URLs
    
    &nbsp;**MapPhase:** 
    1. `master`将Round 1 ReducePhase 输出的**nReduce**个文件，形成**nReduce**个task分发给worker
    2. 每个`worker`收到task后，分别对自己负责的dataChunk中的URL Count排序，得到该dataChunk中top10 most frequent URLs，形成含有10个元素的[]KeyValue，其中Key为URL，Value为URL Count
    3. 每个`worker`根据Key进行分类，把KeyValue写到相应的文件中(共**nReduce**个文件, 此时**nReduce**为1)

     &nbsp;**ReducePhase:** 
     1. `master`将MapPhase中**nMap**个worker输出的**nMap\*nReduce**个文件形成**nReduce**个task分发给worker(此时**nReduce**为1)
     2. 每个`worker`收到task后，将**nMap**个文件中所有URL Count排序，得到Global top10 most frequent URLs
     3. 每个`worker`将Global top10 most frequent URLs写到一个文件中

# 2. PProf
整体优化思路流程如下图所示。

<div align="center">
<img src="./img/pprof.png" height=400 width=350>
</div>

> Phase 1: framework+ExampleURLTop10 -- encoding/JSON耗时太长

完成framework后，用`make test_example`测试了一下，pprof CPU的结果如下图所示。（为了方便，测试了所有的data scale，但genCase只测试了两个，一个随机从0~4个genCase里抽取，另一个随机从5~9个genCase里抽取）

<div align="center">
<img src="./img/phase1.png" height=300 width=500>
</div>
可以明显的看出encoding/JSON花的时间实在是太多了，虽然runtime.systemstack花的时间也不少......然后看了一下内存的分配，如下图所示。内存的分配也非常不合理。先尝试优化encoding/JSON耗时太长的问题。
<div align="center">
<img src="./img/phase1-mem.png" height=250 width=800>
</div>

> Phase 2: framework+URLTop10 -- 对URL分布离散的0~4 genCase，优化基本无效; 5~9 genCase CPU优化近10倍，内存优化近5倍

采用Idea所述的mapF和reduceF后，随机从0~4和5~9 genCase中各随机抽取一个case对所有data scale进行pprof分析，得到结果如下。

首先是`0~4 genCase`的分析。由于0~4 genCase产生的URL分布比较离散，而URLTop10采取的优化主要是在Round1.MapPhase统计URL Count，合并相同的URL从而减小输出JSON文件大小和在Round2.MapPhase对URL Count进行排序，输出局部的Top10 URL上。极端地，如果每个URL只出现一次，那优化就完全没用了，甚至还会因为Round1.MapPhase在统计每个URL Count时的map操作而变慢。(如果先分拣URL，让reduce worker去做count, map操作会更快)。

pprof CPU的输出如下图所示。相比于ExampleURLTop10基本无优化。

<div align="center">
<img src="./img/phase2-0-CPU.png" height=300 width=500>
</div>
pprof mem的输出结果如下图所示。相比于ExampleURLTop10基本无优化。

<div align="center">
<img src="./img/phase2-0-mem.png" height=300 width=900>
</div>

然后是`5~9 genCase`的分析。由于5~9 genCase的URL分布是biased的，MapPhase所做的优化就比较明显了。

pprof CPU的输出如下图所示。相比于ExampleURLTop10优化了将近10倍。由于我在windows上测的结果，所以runtime.cgocall耗时就比较明显了。

<div align="center">
<img src="./img/phase2-5-CPU.png" height=300 width=500>
</div>
pprof mem的输出结果如下图所示。相比于ExampleURLTop10内存优化了将近5倍。

<div align="center">
<img src="./img/phase2-5-mem.png" height=250 width=700>
</div>

> Phase 3: Improved framework + URLTop10 -- 0~4 genCase CPU优化近2倍，内存无明显优化; 5~9 genCase CPU优化将近15倍，内存优化近5倍

此步重点对0~4 genCase进行优化，由前面可知，50%左右的时间都花在了encoding/json上，而需要json编码的只是很简单的一个KeyValue结构体，其实没必要用json编码的。于是此步取消了encoding/json，而采用字符串的分割形式来对MapPhase输出的结果进行读写。0~4 genCase CPU和内存开销比5~9 genCase高太多，所以接下来重点关注0~4 genCase的优化。

0~4 genCase中随机抽取一个gen函数对所有data scale进行测试，pprof CPU的结果如下图所示。可以看到map操作和内存分配占了绝大部分时间。

<div align="center">
<img src="./img/phase3-0-cpu.png" height=300 width=500>
</div>

相应的pprof mem的结果如下图所示。内存只有优化了四五个G左右。其中`URLCountMap`和`fmt.Sprintf`还有`ihash`的耗内存情况比较突出。

<div align="center">
<img src="./img/phase3-0-mem.png" height=300 width=700>
</div>

单独看一下URLCountMap的内存和CPU情况。可以看出用map做URL Count操作是非常昂贵的。
<div align="center">
<img src="./img/phase3-0-list-mem.png" height=250 width=500>
</div>
<div align="center">
<img src="./img/phase3-0-list-cpu.png" height=250 width=500>
</div>

> Phase 4: 内存预分配和底层分配空间复用 -- 内存优化了1.25倍左右

优化的点包括：

1. `bufio.Read(content)`[]byte类型的content复用
2. `map预分配空间`预分配空间大小为len(lines)/4
3. `ihash函数换成了fnvHash64`

**TODO:** 从CPU和mem的结果来看，最应该优化的是fmt.Sprintf，字符串拼接函数，考虑过使用strings.Join、bytes.Buffer等替代方案，但最后会导致bufio.WriteString()写到文件的内容为空。

0~4 genCase随机抽取一个case对所有dataScale进行测试。pprof CPU的结果如下图所示。
<div align="center">
<img src="./img/phase3-0-cpu.png" height=300 width=500>
</div>

相应的pprof mem的结果如下图所示。
<div align="center">
<img src="./img/phase4-0-mem.png" height=400 width=700>
</div>