package edu.whu.tmdb.storage.cache;


import edu.whu.tmdb.storage.utils.K;
import edu.whu.tmdb.storage.utils.V;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

// 以 k-v pair为粒度的缓存
// 采用LRU替换策略
public class DataCache {

    // 最大缓存的k-v数量
    private final int MAX_CACHED_DATA_SIZE = 100000;

    // 哈希表记录k-v
    public TreeMap<K, V> cachedData = new TreeMap();

    // 链表记录插入key的顺序，从而实现LRU
    private LinkedList<K> lruList = new LinkedList<>();


    public V get(K key){
        V ret = this.cachedData.getOrDefault(key, null);

        // 如果key在缓存中，则将key置顶
        if(ret != null){
            this.lruList.remove(key);
            this.lruList.add(key);
        }
        return ret;
    }

    public void put(K key, V value){

        // 如果容量已满，则需要移除最久未使用的
        if(this.cachedData.size() > this.MAX_CACHED_DATA_SIZE){
            K oldKey = this.lruList.pop();
            this.cachedData.remove(oldKey);
        }

        // 新数据加入
        this.cachedData.put(key, value);
        this.lruList.add(key);
    }

}
