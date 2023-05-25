package drz.tmdb.ARIES_log;

import java.io.IOException;
import java.util.Iterator;

import drz.tmdb.cache.DataCache;

public class BufferPool {
    //当前的缓存页
    private DataCache dataCachePool;

    //锁管理器
    private final LockManager lockManager;

    //事务获取不到锁时需要等待，由于实际用的是sleep来体现等待，此处参数是sleep的时间
    private final long SLEEP_INTERVAL;
    public BufferPool() {
        dataCachePool = new DataCache();
        lockManager = new LockManager();
        //太小会造成忙碌的查询死锁，太大会浪费等待时间
        SLEEP_INTERVAL = 500;
    }
    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public static synchronized void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        lockManager.releaseTransactionLocks(tid);
        if (commit) {
            flushPages(tid);
        } else {
            revertTransactionAction(tid);
        }
    }

    /**
     * 在事务回滚时，撤销该事务对page造成的改变
     *
     * @param tid
     */
    public synchronized void revertTransactionAction(TransactionId tid) {
        int size = DataCache.lruList.size();
        for (int i=0; i<size; i++) {
            String key=DataCache.lruList.get(i);
        }
        Iterator<Entry> it = dataCachePool.iterator();
        while (it.hasNext()) {
            Page p = it.next();
            if (p.isDirty() != null && p.isDirty().equals(tid)) {
                lruPagesPool.reCachePage(p.getId());
            }
        }
    }
}
