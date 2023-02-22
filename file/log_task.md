# 	Task-1：根据传入参数写日志

## 1.任务描述

​        在WAL(Write Ahead Log)预写日志技术中，tmdb对对象的所有操作都会先写入日志，请根据传入的参数键（key）、值（value）、操作种类（op）补全写日志方法。

​        提示：利用RandomAccessFile类来实现——Java提供了一个可以对文件随机访问的操作，访问包括读和写操作，该类名为RandomAccessFile。该类的读写是基于指针的操作。

​        日志记录为LogTableItem 对象，该对象结构为：

```java
public class LogTableItem {
    public int logid;//日志记录id
    public Byte op;//0表示插入，1表示删除操作
    public String key;//键
    public String value;//值
    protected long offset;//日志记录在文件中的偏移量

    public LogTableItem(int logid,Byte op,String key,String value){
        this.logid=logid;
        this.op=op;
        this.key=key;
        this.value=value;
    }
    public LogTableItem(){};
}
```

​       给定基本变量并初始化如下：

```java
final File logFile = new File("文件路径" + "文件名.txt");//日志文件
public static RandomAccessFile raf;//RandomAccessFile类的指针
static long currentOffset=0;//目前指针的位置在日志文件中的偏移量
static int currentId=0;//指目前分配的Id号是多少（日志的Id是随着创建LotTableItem对象从0开始递增的）
```

​       当然也欢迎使用别方法来实现。

## 2.实现思路

1、创建文件

可以由RandomAccessFile类实现

2、实现writeLogItemToSSTable方法（传入的参数为LotTableItem对象）

按这个LogTableItem对象的logid、op、key、value、offset顺序写入日志中

相关知识提示：

a. 文件指针操作

- getFilePointer方法

  RandomAccessFile的读写操作都是基于指针的，也就是说总是在指针当前所指向的位置进行读写操作，RandomAccessFile提供了一个可以获取当前指针位置的方法:

  `long getFilePointer()`

  RandomAccessFile在创建时默认指向文件开始(第一个字节)，通过getFilePointer方法获取指针位置时值是"0"。

- seek方法

  RandomAccessFile的提供了一个方法用于移动指针位置：

  `void seek(long pos)`

  使用该方法可以移动指针到指定位置。

b. 文件Write操作

3、实现WriteLog方法（传入参数为日志的key、op、v）

新建LogTableItem对象，并根据传入参数、变量currentID，变量currentOffset设置对象的属性，最后调用writeLogItemToSSTable方法

## 3.结果检验

调用以下loadLog方法检验，查看控制台输出判断是否成功

```java
//模拟写入日志数据
LogManager logManager=new LogManager();
logManager.WriteLog( ,  , );
//...多写几条日志
public void loadLog() throws IOException {
    raf.seek(0);
    for(int i=start;i<currentId;i++){
        System.out.println("id为"+raf.readInt()+" op为"+raf.readByte()+" key为"
                        +raf.readUTF()+" value为"+raf.readUTF()+" offset为"+raf.readLong());
    }
```

# Task-2：根据检查点在崩溃时重做日志

## 1.任务描述

​        检查点作用如下：在基于WAL日志恢复时避免读取所有的日志记录，同时允许删除一些不必要的日志内容。在检查点位置之前的所有日志中记录过的数据变化都已经全部从缓存区中刷入磁盘，反映到了磁盘文件中，从而在宕机恢复时只需从检查点标记之后读取日志并进行恢复，检查点标记之前的日志已经没有意义，可以删除这些日志磁盘文件，在系统崩溃重启时，从检查点之后读取日志并重做。

​        当系统遇到问题而崩溃时，需要查看日志记录进行恢复，把数据库中数据恢复到系统掉电前的那一个时刻，从而使得数据库能正常的启动起来。日志记录了某个对象在数据库中做了什么修改（目前是增加和删除操作）。当系统崩溃时，虽然脏页数据没有持久化，但是日志记录已经持久化到磁盘，接着数据库重启后，可以根据日志中的内容进行重做，将所有数据恢复到最新的状态。结合之前对检查点的描述，可以知道我们需要从检查点之后（即checkpoint+1）位置的日志块开始读取，并根据读取内容对数据库进行恢复。

​        给定基本变量并初始化如下：

```java
public static int checkpoint;//日志检查点
public static long check_off;//检查点在日志中偏移位置
```

​        在脏页统一全部刷盘时，设置检查点：

```java
public void setCheckpoint(){
        checkpoint = currentId;
        check_off = currentOffset;
    }
```

​         请根据检查点写出**加载需重做的所有日志记录**的方法（考虑checkpoint=-1时的情况），并返回需重做的日志记录的LogTableItem对象数组，给出函数头如下：

```java
public LogTableItem[] readRedo() throws IOException {
     //READ REDO LOG
}
```

## 2.实现思路

1、新建LogTableItem对象数组

2、对对象数组进行初始化

3、设置分情况讨论：

- 当checkpoint=-1，没有检查点时，从日志文件开头加载redo log

  需要redo的日志数量为currentId+1，利用raf.readxxx()读日志文件，依次对应LogTableItem对象数组第i个元素的logid、op、key、value、offset，得到每个对象数组元素的各个属性。

- 当checkpoint!=-1，则从检查点后加载redo log

  需要redo的日志数量为currentId-checkpoint，开始读取的地方在日志文件中的偏移量为check_off，其余同上一种情况

4、返回LogTableItem对象数组

## 3.结果检验

```java
//模拟写入日志数据
LogManager logManager=new LogManager();
logManager.WriteLog( ,  , );
//...多写几条日志
logManager.setCheckpoint();//设置检查点
logManager.WriteLog( ,  , );
//...继续写几条日志

//redo测试
 redo_log=logManager.readRedo();
 System.out.println("id为"+redo_log[x].logid+" op为"+redo_log[x].op+" key为"+redo_log[x].key+" value为"+redo_log[x].value+" offset为"+redo_log[x].offset);

```

