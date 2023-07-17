package drz.tmdb.cache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


// 以 k-v pair为粒度的缓存
// 采用LRU替换策略
public class DataCache {

    // 最大缓存的k-v数量
    private final int MAX_CACHED_DATA_SIZE = 100000;

    // 哈希表记录k-v
    public Map<K, V> cachedData = new HashMap<>(MAX_CACHED_DATA_SIZE);

    // 链表记录插入key的顺序，从而实现LRU
    public LinkedList<K> lruList = new LinkedList<>();

    private final long SLEEP_INTERVAL=500;



    public String get(String key) throws InterruptedException {
        K targetKey = new K(key);
        V targetValue = this.cachedData.getOrDefault(targetKey, null);

        if(targetValue == null)
            return null;

        // 如果key在缓存中，则将key置顶
        this.lruList.remove(targetKey);
        this.lruList.add(targetKey);


        return targetValue.valueString;

    }

    public void put(String key, String value) throws InterruptedException {
        // 如果容量已满，则需要移除最久未使用的
        if(this.cachedData.size() > this.MAX_CACHED_DATA_SIZE){
            K oldKey = this.lruList.pop();
            this.cachedData.remove(oldKey);
        }

        // 新数据加入
        this.cachedData.put(new K(key), new V(value));
        this.lruList.add(new K(key));

    }

    public void delete(String key){
        K targetKey = new K(key);
        this.lruList.remove(targetKey);
        cachedData.remove(targetKey);
    }



}
