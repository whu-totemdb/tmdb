package drz.tmdb.ARIES_log;

import android.os.Debug;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

import drz.tmdb.Log.BTree_Indexer;
import drz.tmdb.Log.Constants;
import drz.tmdb.cache.DataCache;
import drz.tmdb.cache.K;
import drz.tmdb.cache.V;


public class LogManager1 {
    public File logFile;//日志文件
    private static RandomAccessFile raf;
    static Boolean recoveryUndecided;
    public static drz.tmdb.memory.MemManager memManager;

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    static long currentOffset = -1;//protected by this
    //    int pageSize;
    static int totalRecords = 0; // for PatchTest //protected by this

    private static BufferPool bufferPool;
    private static DataCache dataCache;

    static HashMap<Long,Long> tidToFirstLogRecord = new HashMap<Long,Long>();


    public LogManager1(drz.tmdb.memory.MemManager memManager) throws IOException {
        this.memManager = memManager;

        bufferPool=new BufferPool();
        dataCache=new DataCache();

        File dir = new File(Constants.LOG_BASE_DIR);
        if(!dir.exists()){
            dir.mkdirs();
        }
        logFile = new File(Constants.LOG_BASE_DIR + "tmdblog");
        if(!logFile.exists())
            logFile.createNewFile();
        raf = new RandomAccessFile(logFile, "rw");
        recoveryUndecided = true;
    }


    public static void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
    }

 //Abort log
    public static void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback
        synchronized (bufferPool) {
            preAppend();

            rollback(tid);
            raf.writeInt(ABORT_RECORD);
            raf.writeLong(tid.getId());
            raf.writeLong(currentOffset);
            currentOffset = raf.getFilePointer();
            force();
            tidToFirstLogRecord.remove(tid.getId());
        }

    }

   //Commit log
    public static synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

 //Update log
    public  synchronized void logWrite(TransactionId tid, K beforeK,V beforeV,K afterK,V afterV)
            throws IOException  {
        preAppend();
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writeEntryData(raf,beforeK.keyString,beforeV.valueString);
        writeEntryData(raf,afterK.keyString,afterV.valueString);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

    }

    void writeEntryData(RandomAccessFile raf, String key, String value) throws IOException{
        raf.writeUTF(key);
        raf.writeUTF(value);
    }



    //Begin log
    public static synchronized  void logXactionBegin(TransactionId tid)
            throws IOException {
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.printf("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();

    }

    //checkpoint log
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (bufferPool) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                bufferPool.flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1);

                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    raf.writeLong(key);
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
            }
        }

        logTruncate();
    }

    //删除不需要的日志
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);


        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();


                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                    case UPDATE_RECORD:
                        String beforeK = raf.readUTF();
                        String beforeV= raf.readUTF();
                        String afterK = raf.readUTF();
                        String afterV= raf.readUTF();

                        writeEntryData(logNew,beforeK,beforeV);
                        writeEntryData(logNew,beforeK,beforeV);
                        break;
                    case CHECKPOINT_RECORD:
                        int numXactions = raf.readInt();
                        logNew.writeInt(numXactions);
                        while (numXactions-- > 0) {
                            long xid = raf.readLong();
                            long xoffset = raf.readLong();
                            logNew.writeLong(xid);
                            logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                        }
                        break;
                    case BEGIN_RECORD:
                        tidToFirstLogRecord.put(record_tid,newStart);
                        break;
                }


                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e) {
                break;
            }
        }

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        //print();
    }




    //本来undo的时候应该逆序重做，但由于update的实现是直接储存整页，因此可以直接取第一次更新前的页
    public static void rollback(TransactionId tid)
            throws NoSuchElementException, IOException {
        synchronized (bufferPool) {
                preAppend();
                long offset = tidToFirstLogRecord.get(tid.getId());
                raf.seek(offset);
                //Stack<Long> stack = new Stack<>();
                while(raf.getFilePointer() < raf.length())
                {
                    LogRecord record = LogRecord.readNext(raf);
                    if(record == null) //|| record instanceof AbortRecord && tid.getId() == record.getTid())
                        break;
                    //获取第一次更新前的页
                    if(record instanceof UpdateRecord && tid.getId() == record.getTid()) {
                        //stack.push(record.getOffset());
                        K beforeK= ((UpdateRecord) record).getBeforeK();
                        V beforeV= ((UpdateRecord) record).getBeforeV();
                        memManager.add(beforeV);
                        dataCache.delete(beforeK.keyString);
                        tidToFirstLogRecord.remove(tid.getId());
                        break;
                    }
                }
                raf.seek(currentOffset);


        }
    }

    //用于recover的undo，相比上面的rollback，采用了逆序undo的方法
    public void rollback(Long tid)
            throws NoSuchElementException, IOException {
        synchronized (bufferPool) {
            synchronized(this) {
                preAppend();
                long offset = tidToFirstLogRecord.get(tid);
                raf.seek(offset);
                Stack<Long> stack = new Stack<>();
                while(raf.getFilePointer() < raf.length())
                {
                    //获取每一条log记录，不同的记录对应不同的record
                    LogRecord record = LogRecord.readNext(raf);
                    if(record == null) //|| record instanceof AbortRecord && tid.getId() == record.getTid())
                        break;
                    //储存对应事务的更新记录
                    if(record instanceof UpdateRecord && tid == record.getTid()) {
                        stack.push(record.getOffset());
                    }
                }
                //逆序撤销所做的更新
                while(!stack.empty())
                {
                    raf.seek(stack.pop());
                    LogRecord record = LogRecord.readNext(raf);
                    K beforeK= ((UpdateRecord) record).getBeforeK();
                    V beforeV= ((UpdateRecord) record).getBeforeV();
                    //将更新前的值写入磁盘
                    memManager.add(beforeV);
                    //丢掉缓存中的键值对
                    dataCache.delete(beforeK.keyString);
                }
            }
        }
    }

    /** Shutdown the logging system, writing out whatever state
     is necessary so that start up can happen quickly (without
     extensive recovery.)
     */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    //ARIES算法流程
    public void recover() throws IOException, InterruptedException, TransactionAbortedException {
        synchronized (bufferPool) {
            synchronized (this) {
                recoveryUndecided = false;
                // Done
                // 1.ANALYSIS PHASE
                // 找到所有avtive的事务，获取最小的offset
                // 可能存在没有checkPoint的情况
                print();
                raf.seek(0);
                HashMap<Long, Long> tid2Offset = new HashMap<>();
                long startOffset = raf.readLong();
                HashMap<Long, List<K>> Keys = new HashMap<>();
                HashMap<K, V> KV = new HashMap<>();
                if(startOffset != -1L)
                {
                    raf.seek(startOffset);
                    LogRecord recordForTid = LogRecord.readNext(raf);
                    if (recordForTid instanceof CheckPointRecord)//&& recordForTid.getOffset() == startOffset)
                    {
                        tid2Offset = ((CheckPointRecord) recordForTid).getTidToFirstLogRecord();
                    } else
                        throw new IOException("CheckPoint pointer points wrong place!");
                }

                Set<Long> Tid = tid2Offset.keySet();
                //将Tid和offset放入tidToFirstLogRecord，因为这些事务都在checkPoint之前开始
                for(Long tid: Tid)
                {
                    tidToFirstLogRecord.put(tid, tid2Offset.get(tid));
                    if(!Keys.containsKey(tid))
                        Keys.put(tid, new ArrayList<>());
                }

                //2.REDO PHASE
                //从checkPoint开始redo所有record
                while(raf.getFilePointer() < raf.length())
                {
                    LogRecord newRecord = LogRecord.readNext(raf);
                    if(newRecord == null)
                        break;
                    //update时先暂存页，就像在bufferPool一样
                    if(newRecord instanceof UpdateRecord)
                    {
                        K afterK = ((UpdateRecord) newRecord).getAfterK();
                        V afterV = ((UpdateRecord) newRecord).getAfterV();
                        KV.put(afterK, afterV);
                        Keys.get(newRecord.getTid()).add(afterK);
                    }
                    //begin时加入tidToFirstLogRecord
                    if(newRecord instanceof BeginRecord)
                    {
                        tidToFirstLogRecord.put(newRecord.getTid(), newRecord.getOffset());
                        Keys.put(newRecord.getTid(), new ArrayList<>());
                    }
                    //abort时删除对应页和tid
                    if(newRecord instanceof AbortRecord)
                    {
                        long tid = newRecord.getTid();
                        tidToFirstLogRecord.remove(tid);
                        ArrayList<K> list = (ArrayList<K>) Keys.get(tid);
                        Keys.remove(tid);
                        for(K id: list)
                        {
                            KV.remove(id);
                        }
                    }
                    //commit时将对应事务的页写入磁盘
                    if(newRecord instanceof CommitRecord)
                    {
                        long tid = newRecord.getTid();
                        tidToFirstLogRecord.remove(tid);
                        ArrayList<K> list = (ArrayList<K>) Keys.get(tid);
                        Keys.remove(tid);
                        for(K id: list)
                        {
                            memManager.add(dataCache.get(id.keyString,new TransactionId(newRecord.getTid())));
                        }
                    }
                }

                // undo Phase
                // 添加ABORT_RECORD，rollback对应事务
                currentOffset = raf.getFilePointer();
                for(Long tid:tidToFirstLogRecord.keySet())
                {
                    raf.seek(currentOffset);
                    raf.writeInt(ABORT_RECORD);
                    raf.writeLong(tid);
                    raf.writeLong(currentOffset);
                    force();
                    rollback(tid);
                    currentOffset = raf.getFilePointer();
                }
                //全部事务都被提交或回滚
                tidToFirstLogRecord.clear();
            }
        }
    }


    public void print() throws IOException {
        raf.seek(0);
        while(raf.getFilePointer() < raf.length())
        {
            System.out.println(LogRecord.readNext(raf));
        }
    }

    public static synchronized void force() throws IOException {
        raf.getChannel().force(true);
    }

}
