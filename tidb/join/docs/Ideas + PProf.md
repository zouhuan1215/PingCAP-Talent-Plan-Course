# 1. Ideas
考虑到MergeJoin要求排序，是全有序的，而HashJoin是先分类，缩小范围后再join。所以在Join Attribute无序的情况下，MergeJoin是要比HashJoin慢的。看了一下`t`文件夹下要排序的文件，第一列是依次递增的，之后的列是无序的。不知道之后正式测试的文件会是什么样，这里还是只考虑了HashJoin。基本思路如下图所示。

<div align="center">
<img src="./img/idea.png" height=300 width=600>
</div>

# 2. PProf
基本思路如下图所示。
<div align="center">
<img src="./img/pprof.png" height=300 width=500>
</div>

> Phase 1: [单线程]--边probe边计算sum

这一步相对于Example的优化主要体现在：

   1. 减少了map的大小。原来在相同key下的map需要存相应row number的slice，现在只需要存这些row的col0的和。
   2. 减少了probe后再用row number去提取相应col0求和的开销。

但是从pprof的结果来看，提升的空间还有很大： a. GC耗时太长；b. CSV读取操作耗时耗内存严重； c. map_assign和map_access耗时太长，map扩容重映射的情况也很严重。pprof CPU和pprof mem的结果分别如下图所示。
<div align="center">
<img src="./img/phase0-cpu.png" height=400 width=600>
</div>
<div align="center">
<img src="./img/phase0-mem.png" height=400 width=600>
</div>

> Phase 2: [单线程]--优化map操作

这一步相对于Example的优化主要体现在：

   1. 用hash(joinAttrs) uint64类型, 替代joinAttrs string类型，作为map key以加速map_access，同时减小map的大小
   2. 预分配map空间，以减少map扩容重映射的时间，减少map_assign的时间

pprof CPU和pprof mem的结果分别如下图所示，可以看到优化后的map耗时减少了1秒左右，内存占用也减少了200M左右。
<div align="center">
<img src="./img/phase2-cpu.png" height=200 width=600>
</div>
<div align="center">
<img src="./img/phase2-mem.png" height=150 width=600>
</div>

> Phase 3: [多线程]--优化CSV文件读取

从之前的pprof结果可以看出：使用`csv`包一行一行地把数据读到一个slice中的方式，在时间上`csv/decoding`十分耗时，在空间上存放所有数据的slice开销巨大。于是决定放弃使用`csv`包，同时为了节省时间，采用读数据和处理数据同步进行的方式。程序中一共有三个角色：`chunkReader`、`master`、`worker`。其中`chunkReader`负责读取数据块，当chunkReader读完一个数据块后会通知master有新的数据块，`master`收到通知后，再去把新的数据块取过来，然后告诉chunkReader可以读下一个数据块了，并同时将新的数据块封装成task分发给worker处理。`worker`处理完自己的数据块后，将结果返还给master，由master汇总输出。处理思路大致如下：

1. `Build Hashtable`. chunkReader负责读数据块，master接收数据块并且处理数据块，建立hashtable。为了内存复用，master有一个dataChunk缓冲区，每次都将从chunkReader取下来的chunk放到这里
2. `Probe`. master负责从chunkReader那里取chunk，并将chunk分发给probeWorker。为了内存复用，master有一个[]dataChunk的缓冲区，在分发task给probeWorker时，会在task中指明该task对应的chunk在\[\]dataChunk中的index，当probeWorker处理完task后，会将这个index和处理结果一起返还给master，告诉master `dataChunk[index]`这个chunk可用了。如果chunkReader读数据比较快，而[]dataChunk中没有可用的chunk了，master才会增大\[\]dataChunk的空间。

pprof CPU和pprof mem的结果分别如下图所示。可以看出，时间上相比于example优化了4倍左右，空间上优化了10倍左右。但最后map操作还是成为了系统的瓶颈。
<div align="center">
<img src="./img/phase3-cpu.png" height=300 width=500>
</div>
<div align="center">
<img src="./img/phase3-mem.png" height=200 width=500>
</div>
<div align="center">
<img src="./img/phase3-list-cpu.png" height=60 width=600>
</div>
<div align="center">
<img src="./img/phase3-list-mem.png" height=100 width=600>
</div>