package edu.whu.tmdb.Log;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class LogWriter {

    final File logFile = new File("D:\\test_data\\" + "tmdbLog");//日志文件

    final private int attrstringlen=8; //属性最大字符串长度为8Byte


    public static int checkpoint;//日志检查点
    public static long check_off;//检查点在日志中偏移位置
    static  long limitedSize=10*1024;//日志文件限制大小
    static  long writeB_size=5*1024;//日志文件限制大小
    static long currentOffset;
    static int currentId;
    static int start;//目前日志的起始id
    static long bTree_off;//bTree加载到内存中的偏移量
    /**
     * 写文件专用线程
     */
    private ExecutorService ioThread;

    /**
     * 缓冲区，使用BlockingQueue就无需另外加锁了；
     */
    private BlockingQueue<String> buffer;

    /**
     * 缓冲区大小
     */
    private static final int BUFFER_SIZE = 100;




    private static class Holder {
        private static LogWriter INSTANCE;

        static {
            try {
                INSTANCE = new LogWriter();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public static LogWriter getInstance() {
        return Holder.INSTANCE;
    }

    private LogWriter() throws FileNotFoundException {
        checkpoint=-1;
        check_off=-1;
        currentOffset = 0;
        currentId = 0;
        start = 0;
        ioThread = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(2));
        buffer = new ArrayBlockingQueue<>(BUFFER_SIZE);
        flushForever();
    }

    //将日志先写入缓存区
    public void writeBuffer(LogTableItem log){
        try{
            buffer.put(Integer.toString(log.logid));
            buffer.put(Byte.toString(log.op));
            buffer.put(log.key);
            buffer.put(log.value);
            buffer.put(Long.toString(log.offset));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 监听写入时机
     */
    private void flushForever() {
        ioThread.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    if (buffer.size() > BUFFER_SIZE - 1 ) {//缓存区写满
                        flush();
                    }
                }
            }
        });
    }

    //给定参数key、op、value，将日志持久化到磁盘
    public void Write(String k,Byte op,String v) {
        LogTableItem LogItem = new LogTableItem(currentId,op,k,v);     //把语句传入logItem，这个时候都是未完成
        LogItem.offset=currentOffset;
        currentId++;
        writeBuffer(LogItem);
    }

    /**
     * 写入，使用BufferedWriter再做一次缓冲
     */
    private void flush() {
        try {
            BufferedWriter fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile, true), StandardCharsets.UTF_8));
            int size = buffer.size();
            for (int i = 0; i < size; i++) {
                fw.write(buffer.take());
            }
            fw.flush();
            fw.close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("flush 成功" );
        }
    }





}
