package drz.tmdb.Log;




import com.alibaba.fastjson.JSONObject;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import drz.tmdb.Level.Constant;
import drz.tmdb.Memory.MemManager;

public class LogManager {
    final File logFile = new File("D:\\test_data\\" + "tmdbLog");//日志文件

    final private int attrstringlen=8; //属性最大字符串长度为8Byte

    public static RandomAccessFile raf;
    public static int checkpoint;//日志检查点
    public static long check_off;//检查点在日志中偏移位置
    static  long limitedSize=10*1024;//日志文件限制大小
    static  long writeB_size=5*1024;//日志文件限制大小
    static long currentOffset;
    static int currentId;
    static int start;//目前日志的起始id
    static long bTree_off;//bTree加载到内存中的偏移量


//    public MemManager memManager=new MemManager();

    //初始化索引b树
    BTree_Indexer<String , Long> bTree_indexer=new BTree_Indexer<>();


    public LogManager() throws IOException {
        //raf = new RandomAccessFile(logFile, "rw");
        checkpoint=-1;
        check_off=-1;
        currentOffset = 0;
        currentId = 0;
        start = 0;
        //app启动时将b树从磁盘加载到内存
        //bTree_indexer=new BTree_Indexer<>("tmdbLog",bTree_off);
    }

    //初始化新日志文件
    public void init() throws IOException {
        logFile.delete();
        File logFile = new File("D:\\test_data\\" + "tmdbLog.txt");
        logFile.createNewFile();
        checkpoint=-1;
        check_off=-1;
        currentOffset = 0;
        currentId = 0;
        start = 0;
    }


    //给定参数LogTableItem对象将日志持久化到磁盘
    public void writeLogItemToSSTable(LogTableItem log){
        try{
            raf.seek(currentOffset);

            raf.writeInt(log.logid);
            raf.writeByte(log.op);
            raf.writeUTF(log.key);
            raf.writeUTF(log.value);
            raf.writeLong(log.offset);
             /**
            byte[] lid=int2Bytes(log.logid, 4);
            raf.write(lid);
            raf.writeByte(log.op);
            raf.writeUTF(log.key);
            raf.writeUTF(log.value);
            byte[] lof=long2Bytes(log.offset);
            raf.write(lof);
              **/
            currentOffset = raf.getFilePointer();
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }
    //给定参数key、op、value，将日志持久化到磁盘
    public void WriteLog(String k,Byte op,String v) throws IOException {
        LogTableItem LogItem = new LogTableItem(currentId,op,k,v);     //把语句传入logItem，这个时候都是未完成
        LogItem.offset=currentOffset;
        currentId++;
        writeLogItemToSSTable(LogItem);
        bTree_indexer.insert(Integer.toString(LogItem.logid),LogItem.offset);//将记录offset的节点插入b树中
        int type=checkFileInSize();
        if(type==1) {//超出日志所存限制，则删除没用的log
            DeleteLog();
        }
        bTree_off= raf.getFilePointer();
        if(type==2){//将索引B树加载到内存中
            bTree_indexer.write("tmdbLog", bTree_off);
        }
    }




    //设置检查点
    public void setCheckpoint(){
        checkpoint = currentId;
        check_off = currentOffset;
    }

    //加载REDO log
    public LogTableItem[] readRedo() throws IOException {
        int redo_num = 0;
        LogTableItem[] redo_log = new LogTableItem[currentId+2];
        //数组初始化
        for(int j=0;j<currentId+2;j++){
            redo_log[j]=new LogTableItem(0, (byte) 0,null,null);
        }
        int i=0;
        if(checkpoint==-1){//还没有检查点，从头开始redo
            long readpos = 0;
            redo_num = currentId+1;
            while (redo_num!=0) {
                raf.seek(readpos);

                redo_log[i].logid = raf.readInt();
                redo_log[i].op = raf.readByte();
                redo_log[i].key = raf.readUTF();
                redo_log[i].value = raf.readUTF();
                redo_log[i].offset = raf.readLong();
                 /**
                byte[] lid=new byte[4];
                raf.read(lid);
                redo_log[i].logid=bytes2Int(lid,0,4);
                redo_log[i].op = raf.readByte();
                redo_log[i].key = raf.readUTF();
                redo_log[i].value = raf.readUTF();
                byte[] lof=new byte[8];
                raf.read(lof);
                redo_log[i].offset=bytes2long(lof);
                  **/
                redo_num--;
                i++;
                readpos = raf.getFilePointer();
            }
        }else {
            redo_num=currentId-checkpoint;
            long readpos = check_off;
            while (redo_num!=0) {
                raf.seek(readpos);

                redo_log[i].logid = raf.readInt();
                redo_log[i].op = raf.readByte();
                redo_log[i].key = raf.readUTF();
                redo_log[i].value = raf.readUTF();
                redo_log[i].offset = raf.readLong();
                 /**
                byte[] lid=new byte[4];
                raf.read(lid);
                redo_log[i].logid=bytes2Int(lid,0,4);
                redo_log[i].op = raf.readByte();
                redo_log[i].key = raf.readUTF();
                redo_log[i].value = raf.readUTF();
                byte[] lof=new byte[8];
                raf.read(lof);
                redo_log[i].offset=bytes2long(lof);
                  **/
                redo_num--;
                i++;
                readpos = raf.getFilePointer();
            }
        }
        return redo_log;
    }
/**
    //REDO
    public void redo() throws IOException {
        int redo_num=currentId-checkpoint;
        LogTableItem[] redo_log = new LogTableItem[redo_num+1];
        //数组初始化
        for(int j=0;j<redo_num;j++){
            redo_log[j]=new LogTableItem(0, (byte) 0,null,null);
        }
        redo_log = readRedo();
        for(int i=0;i<redo_num;i++){
            JSONObject object = JSONObject.parseObject(redo_log[i].value);
            memManager.add(object);
        }
    }
**/

    //检查日志文件大小是否超过限制
    private int checkFileInSize() {
        if(logFile.length()>limitedSize){
            return 1;
        }
        else if(logFile.length()>writeB_size){
            return 2;
        }
        else{
            return 0;
        }
    }

    //删除不必要日志记录（检查点之前的）
    /**
    public void DeleteLog() throws IOException {
        if(check_off!=-1) {
            int n = currentId - checkpoint;
            LogTableItem[] reserve_log = new LogTableItem[n+1];
            //数组初始化
            for(int j=0;j<=n;j++){
                reserve_log[j]=new LogTableItem(0, (byte) 0,null,null);
            }
            int i = 0;
            long readpos = check_off;
            while (n!=0) {
                raf.seek(readpos);
                reserve_log[i].logid = raf.readInt();
                reserve_log[i].op = raf.readByte();
                reserve_log[i].key = raf.readUTF();
                reserve_log[i].value = raf.readUTF();
                reserve_log[i].offset = raf.readLong();
                i++;
                n--;
                readpos = raf.getFilePointer();
            }
            init();
            raf.seek(0);
            for(int j=0;j<=i;j++){//重新写入需保存的日志
                WriteLog(reserve_log[j].key,reserve_log[j].op,reserve_log[j].value);
            }
        }

    }
     **/

    public void DeleteLog() throws IOException {
        try {
            raf.seek(0);
            // 写文件的位置标记,从文件开头开始,后续读取文件内容从该标记开始
            long writePosition = raf.getFilePointer();
            for (int i = 0; i < check_off; i++) {
                Byte b = raf.readByte();
                if (b==null) {
                    break;
                }
            }
            // Shift the next lines upwards.
            // 读文件的位置标记,写完之后回到该标记继续读该行
            long readPosition = raf.getFilePointer();

            // 利用两个标记,
            byte[] buff = new byte[10];
            int n;
            currentOffset=0;
            while (-1 != (n = raf.read(buff))) {
                currentOffset=writePosition;
                long off=currentOffset;
                raf.seek(writePosition);
                raf.write(buff, 0, n);
                readPosition += n;
                writePosition += n;
                raf.seek(readPosition);
            }
            raf.setLength(writePosition);
            raf.seek(logFile.length());//指针还原到更新日志文件末尾
            currentOffset=raf.getFilePointer();//设置现在的currentOffset
            //清除检查点
            check_off=-1;
            checkpoint=-1;
            //起始日志id为checkpoint
            start=checkpoint;
        } catch (IOException e) {
            throw e;
        }
    }

    public void loadLog() throws IOException {
        raf.seek(0);
        for(int i=start;i<currentId;i++){
            System.out.println("id为"+raf.readInt()+" op为"+raf.readByte()+" key为"
                            +raf.readUTF()+" value为"+raf.readUTF()+" offset为"+raf.readLong());
        }
    }

    //寻找指定logid的日志在文件中的位置并返回该日志对象
    public LogTableItem searchLog(int logID) throws IOException {
        long off= bTree_indexer.search(Integer.toString(logID));
        raf.seek(off);
        LogTableItem logItem = new LogTableItem();
        logItem.logid = raf.readInt();
        logItem.op = raf.readByte();
        logItem.key = raf.readUTF();
        logItem.value = raf.readUTF();
        logItem.offset = raf.readLong();
        return logItem;
    }




}
