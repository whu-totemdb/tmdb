package drz.tmdb.sync.timeTest;

public class SendTimeTest {
    private int dataSize;
    private int totalDataSize = 0;

    //发送过程
    private long writeObjectMaxTime = 0;
    private long writeObjectMinTime = Long.MAX_VALUE;
    private long writeObjectTimeOnce;
    private long writeObjectTotalTime = 0;
    private long writeObjectNum = 0;
    private double writeObjectAverageTime;



    private long sendRequestTimeOnce;
    private long sendRequestTotalTime = 0;
    private long sendRequestNum = 0;
    private double sendRequestAverageTime;



    private long processQueueTimeOnce;
    private long processQueueTotalTime = 0;
    private long processQueueTimeNum = 0;
    private double processQueueAverageTime;

    public SendTimeTest(){}



    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
        totalDataSize += dataSize;
        System.out.println(Thread.currentThread().getName() + "：该批请求中一个请求的数据量为：" + dataSize + "B");
    }

    public int getTotalDataSize() {

        System.out.println(Thread.currentThread().getName() + "：该批请求传输的总数据量为：" + totalDataSize + "B");
        return totalDataSize;
    }



    public long getWriteObjectMaxTime() {
        return writeObjectMaxTime;
    }

    public void setWriteObjectMaxTime(long writeObjectMaxTime) {
        this.writeObjectMaxTime = writeObjectMaxTime;
    }

    public long getWriteObjectMinTime() {
        return writeObjectMinTime;
    }

    public void setWriteObjectMinTime(long writeObjectMinTime) {
        this.writeObjectMinTime = writeObjectMinTime;
    }

    public void setWriteObjectTimeOnce(long writeObjectTimeOnce) {
        this.writeObjectTimeOnce = writeObjectTimeOnce;
        writeObjectTotalTime += writeObjectTimeOnce;
        writeObjectNum++;
        System.out.println(Thread.currentThread().getName() + "：进行一次数据序列化花费的时间为：" + writeObjectTimeOnce + "ms");
    }

    public double getWriteObjectAverageTime() {
        writeObjectAverageTime = ((double) writeObjectTotalTime) / ((double)writeObjectNum);

        System.out.println(Thread.currentThread().getName() + "该批请求一共进行了" + writeObjectNum + "次序列化对象操作");
        System.out.println(Thread.currentThread().getName() + "：该批请求进行序列化对象操作花费的总时间为：" + writeObjectTotalTime + "ms");
        System.out.println(Thread.currentThread().getName() + "：该批请求进行序列化对象操作花费的平均时间为：" + writeObjectAverageTime + "ms");

        return writeObjectAverageTime;
    }




    public long getSendRequestTimeOnce() {
        return sendRequestTimeOnce;
    }

    public void setSendRequestTimeOnce(long sendRequestTimeOnce) {
        this.sendRequestTimeOnce = sendRequestTimeOnce;
        sendRequestTotalTime+=sendRequestTimeOnce;
        sendRequestNum++;
        System.out.println(Thread.currentThread().getName() + "：发送一个请求给一个节点需要花费的时间为："+sendRequestTimeOnce+"ms");
    }

    public double getSendRequestAverageTime() {
        sendRequestAverageTime = ((double) sendRequestTotalTime) / ((double)sendRequestNum);

        System.out.println(Thread.currentThread().getName() + "：这批请求一共发送了" + sendRequestNum + "次");
        System.out.println(Thread.currentThread().getName() + "：发送完这批请求给所有节点耗费的总时间为："+sendRequestTotalTime+"ms");
        System.out.println(Thread.currentThread().getName() + "：发送完这批请求给所有节点耗费的平均时间为："+sendRequestAverageTime+"ms");
        return sendRequestAverageTime;
    }




    public long getProcessQueueTimeOnce() {
        return processQueueTimeOnce;
    }

    public void setProcessQueueTimeOnce(long processQueueTimeOnce) {
        this.processQueueTimeOnce = processQueueTimeOnce;
        processQueueTotalTime += processQueueTimeOnce;
        processQueueTimeNum++;
        System.out.println(Thread.currentThread().getName() + "：同步队列处理一次需要花费的时间为："+processQueueTimeOnce+"ms");
    }

    public double getProcessQueueAverageTime() {
        processQueueAverageTime = ((double) processQueueTotalTime) /((double) processQueueTimeNum);

        System.out.println(Thread.currentThread().getName() + "：同步队列处理完这批请求花费的总时间为："+processQueueTotalTime+"ms");
        System.out.println(Thread.currentThread().getName() + "：同步队列处理一次平均花费的时间为："+processQueueAverageTime+"ms");

        return processQueueAverageTime;
    }

    public static long calculate(long start, long end){
        return (end - start);
    }
}
