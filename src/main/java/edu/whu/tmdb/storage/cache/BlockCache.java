package edu.whu.tmdb.storage.cache;


import java.util.LinkedList;
import java.util.TreeMap;

public class BlockCache {

    //  最大缓存的块数量
        private final int MAX_CACHED_BLOCK_COUNT = 10000;

    // 哈希表 ,  块号(SSTable后缀+块下标) ： 块
    public TreeMap<String, byte[]> cachedBlock = new TreeMap();

    // 链表记录插入的顺序，从而实现LRU
    private LinkedList<String> lruList = new LinkedList<>();

    public byte[] get(String blockID){
        return this.cachedBlock.getOrDefault(blockID, null);
    }

    public void put(String blockID, byte[] block){

        // 如果容量已满，则需要移除最久未使用的
        if(this.cachedBlock.size() > this.MAX_CACHED_BLOCK_COUNT){
            String oldBlockID = this.lruList.pop();
            this.cachedBlock.remove(oldBlockID);
        }

        // 新数据加入
        this.cachedBlock.put(blockID, block);
        this.lruList.add(blockID);
    }
}
