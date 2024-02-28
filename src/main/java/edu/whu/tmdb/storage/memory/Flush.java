package edu.whu.tmdb.storage.memory;


import edu.whu.tmdb.storage.level.SSTable;
import edu.whu.tmdb.storage.utils.K;
import edu.whu.tmdb.storage.utils.V;

import java.io.File;
import java.sql.Timestamp;
import java.util.TreeMap;

// 多线程开启flush
public class Flush{

    // 数据
    private TreeMap<K, V> memTable;

    // 文件名后缀
    private int dataFileSuffix;

    private MemManager memManager;


    public Flush(int dataFileSuffix, MemManager memManager){
        this.dataFileSuffix = dataFileSuffix;
        this.memManager = memManager;

        // 开启新的memTable
        this.memTable = memManager.memTable;
        memManager.memTable = new TreeMap<>();

        // 内存清空
        memManager.clearMem();
    }

    public void run() {

        long t1 = System.currentTimeMillis();

        // 生成SSTable对象，将内存中的对象以k-v的形式转移到FileData中
        SSTable sst= new SSTable("SSTable" + dataFileSuffix, 1);
        sst.data = this.memTable;

        // 写SSTable
        long SSTableTotalSize = sst.writeSSTable();

        // 将该SSTable添加到对应level中
        memManager.levelManager.level_0.add(dataFileSuffix);

        // 将该SSTable添加到缓存中
        this.memManager.cacheManager.metaCache.add(sst);
        sst.data.clear(); // 注意清理缓存中sst的数据块，只缓存meta block，否则占用太多内存

        // 将该SSTable添加到levelManager中
        // levelInfo 的结构  dataFileSuffix : level-length-minKey-maxKey
        memManager.levelManager.levelInfo.put("" + dataFileSuffix, "0" + "-" + SSTableTotalSize + "-" + sst.getMinKey() + "-" + sst.getMaxKey());

        long t2 = System.currentTimeMillis();

        // 判断是否触发级联compaction
        try{
            memManager.levelManager.autoCompaction();
        }catch (Exception e){
            e.printStackTrace();
        }


    }
}
