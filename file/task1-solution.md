# TASK1——SSTable读写流程

### 1 LSM-Tree介绍

​		LSM-Tree的英文全称为Log-structured Merge Tree，即日志结构合并树，这是一种分层、有序、面向磁盘的key-value存储结构，其结构如图所示。从其名称出发来理解LSM-Tree，一方面，它是一种基于日志的结构，即所有的操作都要首先写入日志(称为WAL: write ahead log)，这样方便进行数据恢复和异常处理；另一方面，在LSM-Tree中涉及大量合并(merge，也称compaction)操作，数据首先写入内存，如图中的C0所示，随后通过不断的合并过程，在磁盘上从小文件C1不断合并到大文件CL中。

![https://docimg9.docs.qq.com/image/AgAABWe4B1iT90UukmZEn4y1qqfSIoG3.png?w=497&h=190](file:///C:/Users/Lenovo/AppData/Local/Temp/msohtmlclip1/01/clip_image001.png)

​		与传统数据库的就地更新策略(in-place update，即先找到对应数据存储的位置，然后就地修改)不同，LSM-Tree采用一种追加更新(out-place update)的方式，即所有修改操作，都首先以写数据的形式写到内存中，再从内存中顺序刷新(flush)到磁盘。

​		

​		LSM-Tree的具体结构如下图所示。

![https://docimg2.docs.qq.com/image/AgAABWe4B1iEpfP9R61ItLAYm6C34Woy.png?w=436&h=242](file:///C:/Users/Lenovo/AppData/Local/Temp/msohtmlclip1/01/clip_image002.png)

​		在内存中，LSM-Tree的数据保存在MemTable中，MemTable又分为immutable MemTable和mutable MemTable，前者不可修改，后者可修改。数据写入时，会写入mutable MemTable中，当mutable MemTable写满时会转化为immutable MemTable，再写入数据需要等待空闲的mutable MemTable。每个immutable MemTable都会等待合适的时机，进行compaction操作合并到外存中，随后转化为mutable MemTable再次变得可写入。

​		在外存中，LSM-Tree的数据保存在SSTable(Sorted-String Table)中，每个SSTable中的数据按照key排序。这些SSTable分层存储，从level-0到level-n，对应SSTable的数量越来越多，大小越来越大。从内存中的immutable MemTable进行compaction时首先转换为level-0中的SSTable，当level-i的大小达到上限时，会进行compaction到level-(i+1)，经过一系列compaction最终到达level-n。



​		这样的设计带来了很多优点，比如：

​		·卓越的写性能：out-place update不需要首先找到旧数据的实际存储位置，直接写入一条新数据，充分利用磁盘顺序写比随机写效率高；

​		·高空间利用率：按level组织数据，并通过compaction操作将新数据从上层传播到底层，这些compaction操作过滤掉旧版本数据，减少冗余数据大小，从而减少所需的存储空间，提高空间利用率；

​		·版本控制的简化：不同的level提供了天然的版本控制信息，假设level-i中有数据k1-v1， level-(i+1)中有相同key的数据k1-v2，通过层级信息就能判断level-1中的数据是最新版本的数据；

​		·简易的数据恢复：由于WAL的存在，在遇到异常情况时可以通过WAL快速恢复内存中的数据。



​		当然，LSM-Tree这样的设计在获得高效的写性能时，也牺牲了其他方面的性能：

​		·数据的最新程度mutable Memtable > immutable Table > Level-0 > Level-1 > Level-2 >…，因此在查询数据时，需要先扫描MemTable，如果没有命中，则需要从level-0开始一直扫描到level-6直到命中；

​		·LSM-Tree的合并操作(compaction)将若干个SSTable的数据读到内存中，进行更新、去重、排序等一系列操作后重新写回SSTable，涉及到数据的解码、编码、比较、合并,是一个计算密集型的操作。一方面，合并操作在整理数据的过程中会移动数据的位置，导致数据对应的缓存失效，造成了读性能的抖动问题。另一方面，合并操作在整理LSM-Tree数据的同时造成了写放大,严重影响了LSM-Tree的写性能。



### 2 SSTable结构

SSTable (Sorted String Table) 是排序字符串表的简称，它是一个种高效的 key-value 型文件存储格式，其结构如图所示。

![https://docimg5.docs.qq.com/image/AgAABWe4B1hnYYGyBDZPhr_RjkSc9gKi.png?imageMogr2/thumbnail/1600x%3E/ignore-error/1](file:///C:/Users/Lenovo/AppData/Local/Temp/msohtmlclip1/01/clip_image004.jpg)

​		首先在SSTable尾部有个Footer，它给出整个SSTable中其他各区域的物理偏移和长度，也是整个SSTable的访问入口。数据在SSTable中分为不同data block，每个data block为4KB。由于数据在SSTable中按照key排序，因此不同data block之间也严格按照key排序，并把每个data block的最大key保存到index block中。同时，为了优化读性能，我们还额外保存最大key、最小key，和一个Bloom Filter (将在第4小节进行详细介绍)。

​		结合以上对SSTable结构的设计，查询SSTable中指定key的过程如下：

​		①首先访问Footer，获得其他组件的偏移及长度；

​		②访问最小key和最大key，判断要查询的数据在不在这个范围内，若不在则无须继续访问此SSTable；

​		③访问Bloom Filter，输入key值返回true/false，判断对应的数据在不在此SSTable中；

​		④访问index block，其中记录的每个数据块的最大key、偏移、长度，可以根据这些信息定位到目标key可能存在的数据块，随后遍历访问该数据块寻找目标key。



### 3 B树实现索引

​		在tmDB中，索引结构在内存中表现为B-Tree的形式，在磁盘上通过先根遍历B-Tree的所有节点，将其持久化到SSTable中的index block中。

​		B-tree是一种自平衡的树，能够保持数据有序。这种数据结构能够让查找数据、顺序访问、插入数据及删除的动作，都在对数时间内完成，为系统大块数据的读写操作做了优化。B-tree通过减少定位记录时所经历的中间过程，从而加快存取速度。

​		在SSTable的结构中，数据以4KB的大小分为不同data block，在将MemTable持久化为SSTable时，先将每个data block的最大key以及该data block在SSTable中的偏移量插入B-Tree中，得到一个能够记录此SSTable索引信息的B-Tree，如图所示，图中每结点的第一行记录的是data block的最大key，第二行记录的是对应的偏移量，第三行记录的是该结点的孩子结点指针。在此例中k1-k19是递增的。在查询时，假设我们的目标key在k7-k8之间，那么我们首先搜索B-Tree定位到k7和k8，同时确定了目标数据存在于data block 7中，通过偏移量即可进一步访问该data block。

![img](file:///C:/Users/Lenovo/AppData/Local/Temp/msohtmlclip1/01/clip_image004.png)

​		在将B-Tree持久化保存到SSTable中的index block时，需要解决孩子结点如何保存的问题。在内存中的B-Tree，可以很方便地通过指针记录其孩子节点，但是在持久化到磁盘时，指针记录的是在内存中的地址，而我们需要的是孩子结点的磁盘上的物理地址，因此，我们先根遍历B-Tree，即先保存其孩子结点，得到每个孩子结点在SSTable中的物理偏移之后，再保存父节点。例如图中的B-Tree结构，我们先保存A、B、C、D、E结点，同时得到这些节点的偏移量，再记录F结点，同时将第三行的指针替换为实际的物理偏移量。



### 4 Bloom Filter

​		Bloom Filter是由Bloom在1970年提出的一种多哈希函数映射的快速查找算法。通常应用在一些需要快速判断某个元素是否属于集合，但是并不严格要求100%正确的场合。

​		我们在判断某个元素是否属于集合时，通常采用hash set的方法，能够在O(1)的时间复杂度内查询元素是否属于集合。但是随着数据量的增大，会出现大量的哈希冲突，若要减少哈希冲突的概率，则需要扩大哈希数组的大小。若要降低冲突发生的概率到1%以下，则需要将哈希数组的大小设置为100n以上(n为元素总个数)。如此庞大的空间消耗是我们不能接受的，为了拥有O(1)的时间效率，又希望尽量降低空间消耗，我们使用Bloom Filter。

​		Bloom Filter通过使用多个哈希函数，每插入一个元素，将多个bit置为1，在查询时也相应检查对应的多个bit是否全为1，若是则返回true，否则返回false。如图所示，假设我们使用3个哈希函数、18个bit来实现Bloom Filter。当插入元素x时，3个哈希函数计算得到的值分别为1、5、13，相应的我们将这三个bit置为1，同理插入y和z时也会将哈希函数计算得到的bit置为1。此时，如果我们需要判断元素w是否存在集合中，3个哈希函数计算得到的值为4、13、15，我们检查对应的这三个bit，发现不全为1，因此给出结论，元素w不在集合中。

![img](file:///C:/Users/Lenovo/AppData/Local/Temp/msohtmlclip1/01/clip_image005.png)

​		当然，Bloom Filter并不能达到100%的正确率。假如在图中，我们需要判断的元素t恰好通过3个哈希函数计算得到的值为1、3、4，而这三个bit都为1，因此Bloom Filter返回true。但实际上元素t并不存在于集合中。这种情况我们称为假阳性(false positive)。

​		设n为元素个数，p为假阳性概率。为了使p<1%，我们需要合理选择Bloom Filter的参数：

​		·位数组大小m = ![img](file:///C:/Users/Lenovo/AppData/Local/Temp/msohtmlclip1/01/clip_image007.png) ≈ 20n；

​		·哈希函数的个数k =![img](file:///C:/Users/Lenovo/AppData/Local/Temp/msohtmlclip1/01/clip_image009.png) ≈ 14。

​		哈希函数的个数是固定的，只有位数组大小需要随着数据量的变化而变化。因此对于单个SSTable的Bloom Filter部分，我们只需要记录这20n个bit，即⌈20n/8⌉个字节。