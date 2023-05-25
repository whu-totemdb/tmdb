package drz.tmdb.ARIES_log;

import android.os.Debug;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

import drz.tmdb.Log.BTree_Indexer;
import drz.tmdb.Log.Constants;


public class LogManager1 {
    public File logFile;//日志文件
    private static RandomAccessFile raf;
    static Boolean recoveryUndecided;
    public drz.tmdb.memory.MemManager memManager;

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

    static HashMap<Long,Long> tidToFirstLogRecord = new HashMap<Long,Long>();


    public LogManager1(drz.tmdb.memory.MemManager memManager) throws IOException {
        this.memManager = memManager;

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

    /** Write an abort record to the log for the specified tid, force
     the log to disk, and perform a rollback
     @param tid The aborting transaction.
     */
    public static void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

                preAppend();
                //Debug.log("ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                rollback(tid);
                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());

    }

    /** Write a commit record to disk for the specified tid,
     and force the log to disk.

     @param tid The committing transaction.
     */
    public static synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /** Write an UPDATE record to disk for the specified tid and page
     (with provided         before and after images.)
     @param tid The transaction performing the write
     @param before The before image of the page
     @param after The after image of the page

     */
    public  synchronized void logWrite(TransactionId tid, Entry before,
                                       Entry after)
            throws IOException  {
        preAppend();

        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

    }

    void writeEntryData(RandomAccessFile raf, Entry e) throws IOException{
        EntryId eid = e.getId();
        int pageInfo[] = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int i = 0; i < pageInfo.length; i++) {
            raf.writeInt(pageInfo[i]);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object idArgs[] = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = new Integer(raf.readInt());
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException e){
            e.printStackTrace();
            throw new IOException();
        } catch (InstantiationException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
     @param tid The transaction that is beginning

     */
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

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    raf.writeLong(key);
                    //Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    /** Truncate any unneeded portion of the log to reduce its space
     consumption */
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

        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();


                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                    case UPDATE_RECORD:
                        Tuple before = readPageData(raf);
                        Tuple after = readPageData(raf);

                        writePageData(logNew, before);
                        writePageData(logNew, after);
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

                //all xactions finish with a pointer
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

    /** Rollback the specified transaction, setting the state of any
     of pages it updated to their pre-updated state.  To preserve
     transaction semantics, this should not be called on
     transactions that have already committed (though this may not
     be enforced by this method.)

     @param tid The transaction to rollback
     */
    //可以不用LogRecord实现，参照LogTruncate
    //用LogRecord更加简洁
    //2-3补充：本来undo的时候应该逆序重做，但由于update的实现是直接储存整页，因此可以直接取第一次更新前的页
    public static void rollback(TransactionId tid)
            throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
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
                        Tuple tuple = ((UpdateRecord) record).getBefore();
                        Database.getCatalog().getDatabaseFile(tuple.getId().getTableId()).writePage(tuple);
                        Database.getBufferPool().discardPage(tuple.getId());
                        tidToFirstLogRecord.remove(tid.getId());
                        break;
                    }
                }
                raf.seek(currentOffset);
                //逆序撤销所做的更新
                /*while(!stack.empty())
                {
                    raf.seek(stack.pop());
                    LogRecord record = LogRecord.readNext(raf);
                    Page page = ((UpdateRecord) record).getBefore();
                    //将更新前的页写入磁盘
                    //因为在abort前可能进行了checkPoint，使得脏页被写入磁盘
                    Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
                    //丢掉缓存中的页
                    Database.getBufferPool().discardPage(page.getId());
                }*/

            }
        }
    }

    //用于recover的undo，相比上面的rollback，采用了逆序undo的方法
    public void rollback(Long tid)
            throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
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
                        /*raf.seek(record.getOffset());
                        LogRecord newrecord = LogRecord.readNext(raf);
                        Page page = ((UpdateRecord) newrecord).getBefore();
                        Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
                        Database.getBufferPool().discardPage(page.getId());
                        break;*/
                    }
                }
                //逆序撤销所做的更新
                while(!stack.empty())
                {
                    raf.seek(stack.pop());
                    LogRecord record = LogRecord.readNext(raf);
                    Tuple tuple = ((UpdateRecord) record).getBefore();
                    //将更新前的页写入磁盘
                    //因为在abort前可能进行了checkPoint，使得脏页被写入磁盘
                    Database.getCatalog().getDatabaseFile(tuple.getId().getTableId()).writePage(tuple);
                    //丢掉缓存中的页
                    Database.getBufferPool().discardPage(tuple.getId());
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

    /** Recover the database system by ensuring that the updates of
     committed transactions are installed and that the
     updates of uncommitted transactions are not installed.
     */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
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
                HashMap<Long, List<PageId>> pageIds = new HashMap<>();
                HashMap<PageId, Page> pages = new HashMap<>();
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
                    if(!pageIds.containsKey(tid))
                        pageIds.put(tid, new ArrayList<>());
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
                        Page page = ((UpdateRecord) newRecord).getAfter();
                        pages.put(page.getId(), page);
                        pageIds.get(newRecord.getTid()).add(page.getId());
                    }
                    //begin时加入tidToFirstLogRecord
                    if(newRecord instanceof BeginRecord)
                    {
                        tidToFirstLogRecord.put(newRecord.getTid(), newRecord.getOffset());
                        pageIds.put(newRecord.getTid(), new ArrayList<>());
                    }
                    //abort时删除对应页和tid
                    if(newRecord instanceof AbortRecord)
                    {
                        long tid = newRecord.getTid();
                        tidToFirstLogRecord.remove(tid);
                        ArrayList<PageId> list = (ArrayList<PageId>) pageIds.get(tid);
                        pageIds.remove(tid);
                        for(PageId id: list)
                        {
                            pages.remove(id);
                        }
                    }
                    //commit时将对应事务的页写入磁盘
                    if(newRecord instanceof CommitRecord)
                    {
                        long tid = newRecord.getTid();
                        tidToFirstLogRecord.remove(tid);
                        ArrayList<PageId> list = (ArrayList<PageId>) pageIds.get(tid);
                        pageIds.remove(tid);
                        for(PageId id: list)
                        {
                            Database.getCatalog().getDatabaseFile(id.getTableId()).writePage(pages.get(id));
                            pages.get(id).setBeforeImage();
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

    /** Print out a human readable represenation of the log */
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
