package drz.tmdb.sync.timeTest;

public class TimeTest {
    private int dataSize;

    //发送过程
    private long writeObjectTime;

    private long sendRequestAverageTime;

    private long processQueueTimeOnce;

    //接收过程
    private long readObjectTime;

    private long mergeVectorClockTime;





    public TimeTest(){}

    public TimeTest(int dataSize) {
        this.dataSize = dataSize;
    }

    public long getWriteObjectTime() {
        return writeObjectTime;
    }
    public void setWriteObjectTime(long writeObjectTime) {
        this.writeObjectTime = writeObjectTime;
        System.out.println(Thread.currentThread().getName() + "：序列化对象需要花费的时间为："+writeObjectTime+"ms");
    }

    public long getReadObjectTime() {
        return readObjectTime;
    }

    public void setReadObjectTime(long readObjectTime) {
        this.readObjectTime = readObjectTime;
        System.out.println(Thread.currentThread().getName() + "：反序列化对象需要花费的时间为："+readObjectTime+"ms");
    }

    public long getProcessQueueTimeOnce() {
        return processQueueTimeOnce;
    }

    public void setProcessQueueTimeOnce(long processQueueTimeOnce) {
        this.processQueueTimeOnce = processQueueTimeOnce;
        System.out.println(Thread.currentThread().getName() + "：同步队列处理一次需要花费的时间为："+processQueueTimeOnce+"ms");
    }

    public long getMergeVectorClockTime() {
        return mergeVectorClockTime;
    }

    public void setMergeVectorClockTime(long mergeVectorClockTime) {
        this.mergeVectorClockTime = mergeVectorClockTime;
        System.out.println(Thread.currentThread().getName() + "：合并一次向量时钟花费的时间为："+mergeVectorClockTime+"ms");
    }

    public long getSendRequestAverageTime() {
        return sendRequestAverageTime;
    }

    public void setSendRequestAverageTime(long sendRequestAverageTime) {
        this.sendRequestAverageTime = sendRequestAverageTime;
        System.out.println(Thread.currentThread().getName() + "：发送请求给所有节点耗费的平均时间为："+sendRequestAverageTime+"ms");
    }

    public static long calculate(long start, long end){
        return (end - start);
    }
}
