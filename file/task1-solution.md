# TASK1——实现点查询

## 1.任务描述

数据在TMDB中以SSTable的格式持久化保存，请实现函数search(key)实现对特定key的搜索，返回目标key对应的value。

## 2.实现思路

首先请仔细阅读[TMDB总体设计文档]([TMDB总体设计文档2.0.docx](https://1drv.ms/w/s!AkJmIoQ-NhnAmRBfSvLtwBKlohkF?e=m2WCW2))中第二章存储部分的内容，特别是LSM-Tree和SSTable的相关部分。



在一个SSTable中查询特定key对应的value的流程如下：

①首先读取Footer，解析为6个long，依次表示zone map, bloom filter, index block块的开始偏移和长度信息；

②根据①中读取的开始偏移和长度信息，读取、解析zone map块，该部分记录着这张SSTable的min key与max key，比较target key与其大小关系，判断目标数据是否在此SSTable中，若不在则结束，否则进行下一步；

③根据①中读取的开始偏移和长度信息，读取、解析bloom filter块，BloomFilter提供了一个函数boolean check(String key)，此函数能够返回target key是否存在于此SSTable中，若返回false则结束，否则进行下一步；

④根据①中读取的开始偏移和长度信息，读取、解析index block块，index block使用B-Tree实现，记录着每个data block的最大key以及该data block的开始偏移，通过在B-Tree中查找，定位到相应的data block；

⑤在目标data block中遍历查找目标key。



在LSM-Tree中查找特定key对应的value的流程如下：

①先在内存中的MemTable中查找，若未找到则进行下一步；

②在level-0的所有SSTable中依次查找，若未找到则进行下一步；

③在level-1到level-6依次进行步骤②，若未找到则返回空。



## 3.其他说明

该部分作业的代码区主要位于app/src/main/java/drz/tmdb/Level目录下，其中

BloomFilter.java：实现Bloom Filter的功能，对其进行持久化/读取；

BTree.java：实现B-Tree的功能，对其进行持久化/读取；

Constant.java：记录一些常量以及静态方法；

SSTable.java：内存中的SSTable，提供SSTable的读写接口；

LevelManager.java：LSM-Tree层级信息的管理，记录每个level有哪些SSTable等一些元数据，负责执行auto compaction，同时提供manul compaction的接口；

app/src/main/java/drz/tmdb/Memory/MemManager.java：内存管理，提供往MemTable中加入数据的接口，同时负责MemTable写满时将其写成SSTable。



在SSTable.java中已经实现了SSTable的写入以及SSTable中meta data (即zone map, bloom filter, index block, footer)的读取的代码，可以作为部分参考。



最终要实现的是MemManager.java中的函数search(key)。

