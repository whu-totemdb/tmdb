package drz.tmdb.cache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

// 以 k-v pair为粒度的缓存
// 采用LRU替换策略
public class DataCache {

    // 最大缓存的k-v数量
    private final int MAX_CACHED_DATA_SIZE = 100000;

    // 哈希表记录k-v
    public Map<String, String> cachedData = new HashMap<>(MAX_CACHED_DATA_SIZE);

    // 链表记录插入key的顺序，从而实现LRU
    private LinkedList<String> lruList = new LinkedList<>();


    public String get(String key){
        String ret = this.cachedData.getOrDefault(key, null);

        // 如果key在缓存中，则将key置顶
        if(ret != null){
            this.lruList.remove(key);
            this.lruList.add(key);
        }
        return ret;
    }

    public void put(String key, String value){

        // 如果容量已满，则需要移除最久未使用的
        if(this.cachedData.size() > this.MAX_CACHED_DATA_SIZE){
            String oldKey = this.lruList.pop();
            this.cachedData.remove(oldKey);
        }

        // 新数据加入
        this.cachedData.put(key, value);
        this.lruList.add(key);
    }

}
