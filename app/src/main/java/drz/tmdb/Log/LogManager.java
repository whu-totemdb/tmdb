package drz.tmdb.Log;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import drz.tmdb.Memory.MemManager;
import drz.tmdb.Memory.Tuple;

public class LogManager {
    public File logFile;//日志文件

    public File fileTree;//存B树文件
    BufferedOutputStream bTreeWriteAccess;

    public static RandomAccessFile logIOAccess;
    public static int checkpoint;//日志检查点
    public static long check_off;//检查点在日志中偏移位置
    static  long limitedSize=10*10*1024;//日志文件限制大小
    static  long writeB_size=5*10*1024;//写b树时日志文件大小
    static long currentOffset;
    static int currentId;

    public static final int MAX_BUFFER_SIZE = 1000;  // 设置Log Buffer的最大容量
    public static final List<LogTableItem> logBuffer = new ArrayList<>();  // Log Buffer



    public MemManager memManager;

    //初始化索引b树
    public BTree_Indexer<String , Long> bTree_indexer;

    //建立hashmap将日志记录按对象分类
    public static Map< String, List<Integer>> Map;
    //遍历hashMap的keySet
    public Iterator<Map.Entry<String, List<Integer>>> iterator;

    public LogManager(MemManager memManager) throws IOException {
        this.memManager = memManager;

        File dir = new File(Constants.LOG_BASE_DIR);
        if(!dir.exists()){
            dir.mkdirs();
        }
        logFile = new File(Constants.LOG_BASE_DIR + "tmdblog");
        fileTree = new File(Constants.LOG_BASE_DIR + "log_btree");
        if(!logFile.exists())
            logFile.createNewFile();
        if(!fileTree.exists())
            fileTree.createNewFile();
        bTreeWriteAccess = new BufferedOutputStream(new FileOutputStream(fileTree));
        logIOAccess = new RandomAccessFile(logFile, "rw");
        Map= new HashMap < String, List<Integer> > ();
        checkpoint=-1;
        check_off=-1;
        currentOffset = 0;
        currentId = 0;
        //app启动时将b树从磁盘加载到内存
        if(fileTree.length()==0){
            bTree_indexer = new BTree_Indexer<>();
        }else{
            bTree_indexer=new BTree_Indexer<>("log_btree",0);
        }
    }

    //初始化新日志文件与新索引b树文件
    public void init() throws IOException {
        Map.clear();//删除Map中所有键值对
        if(deleteDirectory(Constants.LOG_BASE_DIR)) {
            File dir = new File(Constants.LOG_BASE_DIR);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            logFile = new File(Constants.LOG_BASE_DIR + "tmdblog");
            fileTree = new File(Constants.LOG_BASE_DIR + "log_btree");
            if (!logFile.exists())
                logFile.createNewFile();
            if (!fileTree.exists())
                fileTree.createNewFile();
            bTreeWriteAccess = new BufferedOutputStream(new FileOutputStream(fileTree));
            logIOAccess = new RandomAccessFile(logFile, "rw");
            checkpoint = -1;
            check_off = -1;
            currentOffset = 0;
            currentId = 0;
            //app启动时将b树从磁盘加载到内存
            if (fileTree.length() == 0) {
                bTree_indexer = new BTree_Indexer<>();
            } else {
                bTree_indexer = new BTree_Indexer<>("log_btree", 0);
            }
        }
    }

    //删除dir目录及下面的两个文件
    public static boolean deleteDirectory(String dir) {
        // 如果dir不以文件分隔符结尾，自动添加文件分隔符
        if (!dir.endsWith(File.separator))
            dir = dir + File.separator;
        File dirFile = new File(dir);
        // 如果dir对应的文件不存在，或者不是一个目录，则退出
        if ((!dirFile.exists()) || (!dirFile.isDirectory())) {
            System.out.println("删除目录失败：" + dir + "不存在！");
            return false;
        }
        boolean flag = true;
        // 删除文件夹中的所有文件包括子目录
        File[] files = dirFile.listFiles();
        for (int i = 0; i < files.length; i++) {
            // 删除子文件
            if (files[i].isFile()) {
                flag = deleteFile(files[i].getAbsolutePath());
                if (!flag)
                    break;
            }
        }
        if (!flag) {
            System.out.println("删除目录失败！");
            return false;
        }
        // 删除当前目录
        if (dirFile.delete()) {
            System.out.println("删除目录" + dir + "成功！");
            return true;
        } else {
            return false;
        }
    }

    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        // 如果文件路径所对应的文件存在，并且是一个文件，则直接删除
        if (file.exists() && file.isFile()) {
            if (file.delete()) {
                System.out.println("删除单个文件" + fileName + "成功！");
                return true;
            } else {
                System.out.println("删除单个文件" + fileName + "失败！");
                return false;
            }
        } else {
            System.out.println("删除单个文件失败：" + fileName + "不存在！");
            return false;
        }
    }



    //给定参数LogTableItem对象将日志持久化到磁盘
    public void writeLogItemToSSTable(LogTableItem log){
        try{
            logIOAccess.seek(currentOffset);

            if(log instanceof LogTableItem.LogRecord){
                logIOAccess.writeInt(log.logid);
                logIOAccess.writeInt(log.txn_id);
                logIOAccess.writeByte(((LogTableItem.LogRecord) log).op);
                logIOAccess.writeUTF(((LogTableItem.LogRecord) log).key);
                logIOAccess.writeUTF(((LogTableItem.LogRecord) log).value);
                logIOAccess.writeLong(log.offset);
            }else{
                logIOAccess.writeInt(log.logid);
                logIOAccess.writeInt(log.txn_id);
                logIOAccess.writeLong(log.offset);
            }

            currentOffset = logIOAccess.getFilePointer();

        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    //给定参数key、op、value，将日志写入logBuffer
    public void WriteLogRecord(String k,int txn_id,Byte op,String v){
        LogTableItem LogItem = new LogTableItem.LogRecord(currentId,txn_id,op,k,v);     //把语句传入logItem，这个时候都是未完成
        LogItem.offset=currentOffset;
        currentId++;
        logBuffer.add(LogItem);//先将日志写入LogBuffer中
        if (logBuffer.size() >= MAX_BUFFER_SIZE) {
            flushBuffer();  // 如果Log Buffer已满，就将其刷盘
        }
    }

    public void WriteLog(int txn_id){
        LogTableItem LogItem = new LogTableItem(currentId,txn_id);     //把语句传入logItem，这个时候都是未完成
        LogItem.offset=currentOffset;
        currentId++;
        logBuffer.add(LogItem);//先将日志写入LogBuffer中
        if (logBuffer.size() >= MAX_BUFFER_SIZE) {
            flushBuffer();  // 如果Log Buffer已满，就将其刷盘
        }
    }

    //logBuffer写满后将其中的所有日志持久化到磁盘
    public void flushBuffer(){
        for(LogTableItem logRecord:logBuffer){
            int flag=0;
            writeLogItemToSSTable(logRecord);
            System.out.println("该条日志已写入，详细信息为：" + logRecord);

            iterator = Map.entrySet().iterator();
            //将该日志记录的logid按对象分类
            if(logRecord instanceof LogTableItem.LogRecord) {
                while (iterator.hasNext()) {
                    Map.Entry<String, List<Integer>> entry = iterator.next();
                    if (((LogTableItem.LogRecord) logRecord).value == entry.getKey()) {
                        entry.getValue().add(logRecord.logid);
                        flag = 1;
                        break;
                    }
                }
                if (flag == 0) {
                    List<Integer> list = new ArrayList<Integer>();//hashmap里没找到该对象则新建一个列表
                    list.add(logRecord.logid);
                    Map.put(((LogTableItem.LogRecord) logRecord).value, list);
                }
            }

            bTree_indexer.insert(Integer.toString(logRecord.logid),logRecord.offset);//将记录offset的节点插入b树中
        }
        int type=checkFileInSize();
        if(type==2){//将索引B树持久化到磁盘
            bTree_indexer.write(bTreeWriteAccess, 0);
        }

    }


    //设置检查点
    public void setCheckpoint() throws IOException {
        int type=checkFileInSize();
        if(type==1){//新建日志文件与索引b树文件重新写
            init();
        }else {
            checkpoint = currentId;
            check_off = currentOffset;
        }
        System.out.println("内存中数据刷盘，设置检查点！");
    }

    //加载REDO log
    public LogTableItem[] readRedo() throws IOException {
        int redo_num = 0;
        LogTableItem[] redo_log = new LogTableItem[currentId+2];
        //数组初始化
        for(int j=0;j<currentId+2;j++){
            redo_log[j]=new LogTableItem(0, (byte) 0);
        }
        int i=0;
        if(checkpoint==-1){//还没有检查点，从头开始redo
            long readpos = 0;
            redo_num = currentId+1;
            while (redo_num!=0) {
                logIOAccess.seek(readpos);
                if(redo_log[i] instanceof LogTableItem.LogRecord){
                    redo_log[i].logid = logIOAccess.readInt();
                    redo_log[i].txn_id = logIOAccess.readByte();
                    ((LogTableItem.LogRecord) redo_log[i]).op = logIOAccess.readByte();
                    ((LogTableItem.LogRecord) redo_log[i]).key = logIOAccess.readUTF();
                    ((LogTableItem.LogRecord) redo_log[i]).value = logIOAccess.readUTF();
                    redo_log[i].offset = logIOAccess.readLong();
                }else{
                    redo_log[i].logid = logIOAccess.readInt();
                    redo_log[i].txn_id = logIOAccess.readByte();
                    redo_log[i].offset = logIOAccess.readLong();
                }

                redo_num--;
                i++;
                readpos = logIOAccess.getFilePointer();
            }
        }else {
            redo_num=currentId-checkpoint;
            long readpos = check_off;
            while (redo_num!=0) {
                logIOAccess.seek(readpos);

                if(redo_log[i] instanceof LogTableItem.LogRecord){
                    redo_log[i].logid = logIOAccess.readInt();
                    redo_log[i].txn_id = logIOAccess.readByte();
                    ((LogTableItem.LogRecord) redo_log[i]).op = logIOAccess.readByte();
                    ((LogTableItem.LogRecord) redo_log[i]).key = logIOAccess.readUTF();
                    ((LogTableItem.LogRecord) redo_log[i]).value = logIOAccess.readUTF();
                    redo_log[i].offset = logIOAccess.readLong();
                }else{
                    redo_log[i].logid = logIOAccess.readInt();
                    redo_log[i].txn_id = logIOAccess.readByte();
                    redo_log[i].offset = logIOAccess.readLong();
                }
                i++;
                readpos = logIOAccess.getFilePointer();
            }
        }
        return redo_log;
    }

    //REDO
    public void redo() throws IOException {
        int redo_num = currentId - checkpoint;
        LogTableItem[] redo_log = new LogTableItem[redo_num + 1];
        //数组初始化
        for (int j = 0; j < redo_num; j++) {
            redo_log[j] = new LogTableItem(0, (byte) 0);
        }
        redo_log = readRedo();
        for (int i = 0; i < redo_num; i++) {
            if (redo_log[i] instanceof LogTableItem.LogRecord) {
                Tuple t = JSON.parseObject(((LogTableItem.LogRecord) redo_log[i]).value, Tuple.class);
                System.out.println("崩溃后redo，数据重新恢复到数据库中！");
                memManager.add(t);
            }
        }
    }



    //检查日志文件大小是否超过限制
    public int checkFileInSize(){
        if(logFile.length()>limitedSize){
            return 1;
        }
        if(logFile.length()>writeB_size){
            return 2;
        }
        else{
            return 0;
        }
    }



    public void loadLog() throws IOException {
        logIOAccess.seek(0);
        for(int i=0;i<currentId;i++){
            System.out.println("id为"+ logIOAccess.readInt()+" op为"+ logIOAccess.readByte()+" key为"
                    + logIOAccess.readUTF()+" value为"+ logIOAccess.readUTF()+" offset为"+ logIOAccess.readLong());
        }
    }

    //寻找指定logid的日志在文件中的位置并返回该日志对象
    public LogTableItem searchLog(int logID) throws IOException {
        long off= bTree_indexer.search(Integer.toString(logID));
        logIOAccess.seek(off);
        LogTableItem logItem = new LogTableItem();
        logItem.logid = logIOAccess.readInt();
        logItem.op = logIOAccess.readByte();
        logItem.key = logIOAccess.readUTF();
        logItem.value = logIOAccess.readUTF();
        logItem.offset = logIOAccess.readLong();
        return logItem;
    }





}
