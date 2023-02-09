package drz.tmdb.Log;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;

import drz.tmdb.Level.Constant;

public class LogManager {
    final File logFile = new File("D:\\test_data\\" + "tmdbLog.txt");//日志文件

    final private int attrstringlen=8; //属性最大字符串长度为8Byte

    public static RandomAccessFile raf;
    public static int checkpoint;//日志检查点
    public static long check_off;//检查点在日志中偏移位置
    static  long limitedSize=10*1024*1024;//日志文件限制大小
    static long currentOffset;
    static int currentId;

    HashMap<Integer, LogTableItem> map = new HashMap<Integer, LogTableItem>(){
        {
            put(null,null);
            put(null,null);
        }
    };//logid与日志对象对应

    public LogManager() throws IOException {
        raf = new RandomAccessFile(logFile, "rw");
        checkpoint=-1;
        check_off=-1;
        currentOffset = 0;
        currentId = 0;
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
    }

    public void writeLogItemToSSTable(LogTableItem log){
        try{
            raf.seek(currentOffset);

            raf.writeInt(log.logid);
            raf.writeByte(log.op);
            raf.writeInt(log.length);
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
    public void WriteLog(String k,Byte op,String v){
        LogTableItem LogItem = new LogTableItem(currentId,op,k,v);     //把语句传入logItem，这个时候都是未完成
        LogItem.offset=currentOffset;
        map.put(LogItem.logid,LogItem);
        currentId++;
        writeLogItemToSSTable(LogItem);
    }

    //设置检查点
    public void setCheckpoint(){
        checkpoint = currentId;
        check_off = currentOffset;
    }

    //REDO
    public LogTableItem[] redo() throws IOException {
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
                redo_log[i].length=raf.readInt();
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
                redo_log[i].length=raf.readInt();
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

    //检查日志文件大小是否超过限制
    private boolean checkFileInSize() {
        return logFile.length() <= limitedSize;
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
        } catch (IOException e) {
            throw e;
        }
    }


    //编码int为byte
    private byte[] int2Bytes(int value, int len){
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[len - i - 1] = (byte)(value >> 8 * i);
        }
        return b;
    }

    //编码long为byte
    public static byte[] long2Bytes(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }


    //解码byte为int
    private int bytes2Int(byte[] b, int start, int len) {
        int sum = 0;
        int end = start + len;
        for (int i = start; i < end; i++) {
            int n = b[i]& 0xff;
            n <<= (--len) * 8;
            sum += n;
        }
        return sum;
    }

    //解码byte为long
    public static long bytes2long(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();
        return buffer.getLong();
    }

}
