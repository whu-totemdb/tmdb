package drz.tmdb.ARIES_log;

import org.apache.commons.collections4.list.TreeList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import drz.tmdb.cache.DataCache;
import drz.tmdb.cache.K;
import drz.tmdb.cache.V;
import drz.tmdb.level.SSTable;

public class BufferPool {
    //当前的缓存页
    private static DataCache dataCachePool;

    //锁管理器
    private static LockManager lockManager;

    private static drz.tmdb.memory.MemManager memManager;

    public static List<LogRecord> logBuffer = new ArrayList<>();

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

    private static void flushPages(TransactionId tid){

        List<V> kvPairsToSST = new TreeList<>(); // 暂存需要刷盘的键值对

        Iterator <Map.Entry<K,V>> iterator = dataCachePool.cachedData.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry< K,V > entry = iterator.next();
            if(entry.getValue().isDirty()!=null && entry.getValue().lastDirtyOperation==tid){
                kvPairsToSST.add(entry.getValue());
            }
        }

        flushPage(kvPairsToSST);
    }


    private static void flushPage(List<V> kvPairsToSST){
        for(V v : kvPairsToSST){
            memManager.add(v);
        }
        try{
            memManager.saveMemTableToFile();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void flushAllPages(){
        List<V> kvPairsToSST=new ArrayList<>();
        Iterator <Map.Entry<K,V>> iterator = dataCachePool.cachedData.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry< K,V > entry = iterator.next();
            if(entry.getValue().isDirty()!=null){
                kvPairsToSST.add(entry.getValue());
            }
        }
        flushPage(kvPairsToSST);
    }
}
