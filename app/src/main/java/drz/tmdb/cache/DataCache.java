package drz.tmdb.cache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import drz.tmdb.ARIES_log.LockManager;
import drz.tmdb.ARIES_log.Transaction;
import drz.tmdb.ARIES_log.TransactionAbortedException;
import drz.tmdb.ARIES_log.TransactionId;

// 以 k-v pair为粒度的缓存
// 采用LRU替换策略
public class DataCache {
    private static LockManager lockManager=new LockManager();

    // 最大缓存的k-v数量
    private final int MAX_CACHED_DATA_SIZE = 100000;

    // 哈希表记录k-v
    public Map<K, V> cachedData = new HashMap<>(MAX_CACHED_DATA_SIZE);

    // 链表记录插入key的顺序，从而实现LRU
    public LinkedList<K> lruList = new LinkedList<>();

    private final long SLEEP_INTERVAL=500;



    public String get(String key, TransactionId tid) throws InterruptedException, TransactionAbortedException {
        K k=new K(key);
        boolean result = lockManager.grantSLock(tid, k);
        //下面的while循环就是在模拟等待过程，隔一段时间就检查一次是否申请到锁了，还没申请到就检查是否陷入死锁
        while (!result) {
            if (lockManager.deadlockOccurred(tid, k)) {
                throw new TransactionAbortedException();
            }
            Thread.sleep(SLEEP_INTERVAL);
            //sleep之后再次判断result
            result = lockManager.grantSLock(tid, k);
        }

        K targetKey = new K(key);
        V targetValue = this.cachedData.getOrDefault(targetKey, null);

        if(targetValue == null)
            return null;

        // 如果key在缓存中，则将key置顶
        this.lruList.remove(targetKey);
        this.lruList.add(targetKey);

        return targetValue.valueString;

    }

    public void put(String key, String value,TransactionId tid) throws TransactionAbortedException, InterruptedException {
        K k=new K(key);
        boolean result = lockManager.grantXLock(tid, k);
        //下面的while循环就是在模拟等待过程，隔一段时间就检查一次是否申请到锁了，还没申请到就检查是否陷入死锁
        while (!result) {
            if (lockManager.deadlockOccurred(tid, k)) {
                throw new TransactionAbortedException();
            }
            Thread.sleep(SLEEP_INTERVAL);
            //sleep之后再次判断result
            result = lockManager.grantSLock(tid, k);
        }

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

    public synchronized void releaseKey(TransactionId tid, String key) {
        K k=new K(key);
        if (!lockManager.unlock(tid, k)) {
            throw new IllegalArgumentException();
        }
    }

}
