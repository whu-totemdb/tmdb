package drz.tmdb.ARIES_log;

import java.io.IOException;
import java.util.Iterator;

import drz.tmdb.cache.DataCache;
import drz.tmdb.cache.K;
import drz.tmdb.cache.V;

public class BufferPool {
    //当前的缓存页
    private DataCache dataCachePool;

    //锁管理器
    private static LockManager lockManager;

    private static drz.tmdb.memory.MemManager memManager;

    //事务获取不到锁时需要等待，由于实际用的是sleep来体现等待，此处参数是sleep的时间
    private final long SLEEP_INTERVAL;
    public BufferPool() throws IOException {
        dataCachePool = new DataCache();
        lockManager = new LockManager();
        memManager=new drz.tmdb.memory.MemManager();
        //太小会造成忙碌的查询死锁，太大会浪费等待时间
        SLEEP_INTERVAL = 500;
    }
    /**
     * 事务commmit或abort时调用，释放所有锁
     *
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public static synchronized void transactionComplete(TransactionId tid, boolean commit)
            throws IOException, InterruptedException, TransactionAbortedException {
        lockManager.releaseTransactionLocks(tid);
        if (commit) {
           // flushPages(tid);
        } else {
            revertTransactionAction(tid);
        }
    }

    /**
     * 在事务回滚时，撤销该事务对page造成的改变
     *
     * @param tid
     */
    public static synchronized void revertTransactionAction(TransactionId tid) throws InterruptedException, TransactionAbortedException {
        int size = memManager.cacheManager.dataCache.lruList.size();
        for (int i=0; i<size; i++) {
            K key=memManager.cacheManager.dataCache.lruList.get(i);
            V value=memManager.cacheManager.dataCache.cachedData.get(key);
            if(value.isDirty()!=null&&value.isDirty().equals(tid)){
                String oldValue= (String)memManager.search(key.keyString);
                memManager.cacheManager.dataCache.put(key.keyString,oldValue,tid);
            }
        }

    }
}
