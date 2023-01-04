package drz.oddb.sync.timeTest;

public class SendTimeTest {
    private int dataSize;

    //发送过程
    private long writeObjectMaxTime = 0;

    private long writeObjectMinTime = Long.MAX_VALUE;

    private long writeObjectTotalTime = 0;

    private long writeObjectAverageTime;

    private long sendRequestAverageTime;

    private long processQueueTimeOnce;

    public SendTimeTest(){}

    public SendTimeTest(int dataSize) {
        this.dataSize = dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public long getWriteObjectMaxTime() {
        return writeObjectMaxTime;
    }

    public void setWriteObjectMaxTime(long writeObjectMaxTime) {
        this.writeObjectMaxTime = writeObjectMaxTime;
        writeObjectTotalTime += writeObjectMaxTime;
    }

    public long getWriteObjectMinTime() {
        return writeObjectMinTime;
    }

    public void setWriteObjectMinTime(long writeObjectMinTime) {
        this.writeObjectMinTime = writeObjectMinTime;
        this.writeObjectTotalTime += writeObjectMinTime;
    }

    /*public void setWriteObjectTime(long writeObjectTime) {
        this.writeObjectTime = writeObjectTime;
        System.out.println(Thread.currentThread().getName() + "：序列化对象需要花费的时间为："+writeObjectTime+"ms");
        WriteObjectTotalTime += writeObjectTime;
    }*/

    public long getWriteObjectTotalTime() {
        return writeObjectTotalTime;
    }

    public void setWriteObjectTotalTime(long writeObjectTotalTime) {
        this.writeObjectTotalTime = writeObjectTotalTime;
    }

    public long getWriteObjectAverageTime() {
        return writeObjectAverageTime;
    }

    public void setWriteObjectAverageTime(long writeObjectAverageTime) {
        this.writeObjectAverageTime = writeObjectAverageTime;
        System.out.println(Thread.currentThread().getName() + "：序列化对象需要花费的平均时间为："+writeObjectAverageTime+"ms");
    }

    public long getSendRequestAverageTime() {
        return sendRequestAverageTime;
    }

    public void setSendRequestAverageTime(long sendRequestAverageTime) {
        this.sendRequestAverageTime = sendRequestAverageTime;
        System.out.println(Thread.currentThread().getName() + "：发送请求给所有节点耗费的平均时间为："+sendRequestAverageTime+"ms");
    }

    public long getProcessQueueTimeOnce() {
        return processQueueTimeOnce;
    }

    public void setProcessQueueTimeOnce(long processQueueTimeOnce) {
        this.processQueueTimeOnce = processQueueTimeOnce;
        System.out.println(Thread.currentThread().getName() + "：同步队列处理一次需要花费的时间为："+processQueueTimeOnce+"ms");
    }

    public static long calculate(long start, long end){
        return (end - start);
    }
}
