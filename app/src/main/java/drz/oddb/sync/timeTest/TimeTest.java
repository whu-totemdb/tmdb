package drz.oddb.sync.timeTest;

public class TimeTest {
    private static long writeObjectTime;

    private static long objectInputStreamInitialTime;

    private static long readObjectTime;

    private static long processQueueTimeOnce;

    private static long mergeVectorClockTime;

    private static long generateRequestTime;

    private static long getAliveNodesTime;

    private static long requestsHasSent;

    private static long sendRequestTime;

    public static long getWriteObjectTime() {
        return writeObjectTime;
    }

    public static void setWriteObjectTime(long writeObjectTime) {
        TimeTest.writeObjectTime = writeObjectTime;
        System.out.println(Thread.currentThread().getName() + "：序列化对象需要花费的时间为："+writeObjectTime+"ms");
    }

    public static long getObjectInputStreamInitialTime() {
        return objectInputStreamInitialTime;
    }

    public static void setObjectInputStreamInitialTime(long objectInputStreamInitialTime) {
        TimeTest.objectInputStreamInitialTime = objectInputStreamInitialTime;
        System.out.println(Thread.currentThread().getName() + "：对象输入流初始化需要花费的时间为："+objectInputStreamInitialTime+"ms");
    }

    public static long getReadObjectTime() {
        return readObjectTime;
    }

    public static void setReadObjectTime(long readObjectTime) {
        TimeTest.readObjectTime = readObjectTime;
        System.out.println(Thread.currentThread().getName() + "：反序列化对象需要花费的时间为："+readObjectTime+"ms");
    }

    public static long getProcessQueueTimeOnce() {
        return processQueueTimeOnce;
    }

    public static void setProcessQueueTimeOnce(long processQueueTimeOnce) {
        TimeTest.processQueueTimeOnce = processQueueTimeOnce;
        System.out.println(Thread.currentThread().getName() + "：同步队列处理一次需要花费的时间为："+processQueueTimeOnce+"ms");
    }

    public static long getMergeVectorClockTime() {
        return mergeVectorClockTime;
    }

    public static void setMergeVectorClockTime(long mergeVectorClockTime) {
        TimeTest.mergeVectorClockTime = mergeVectorClockTime;
        System.out.println(Thread.currentThread().getName() + "：合并一次向量时钟花费的时间为："+mergeVectorClockTime+"ms");
    }

    public static long getGenerateRequestTime() {
        return generateRequestTime;
    }

    public static void setGenerateRequestTime(long generateRequestTime) {
        TimeTest.generateRequestTime = generateRequestTime;
        System.out.println(Thread.currentThread().getName() + "：生成一次请求花费的时间为："+generateRequestTime+"ms");
    }

    public static long getGetAliveNodesTime() {
        return getAliveNodesTime;
    }

    public static void setGetAliveNodesTime(long getAliveNodesTime) {
        TimeTest.getAliveNodesTime = getAliveNodesTime;
        System.out.println(Thread.currentThread().getName() + "：获取一次活跃节点花费的时间为："+getAliveNodesTime+"ms");
    }

    public static long getRequestsHasSent() {
        return requestsHasSent;
    }

    public static void setRequestsHasSent(long requestsHasSent) {
        TimeTest.requestsHasSent = requestsHasSent;
        System.out.println(Thread.currentThread().getName() + "：请求完成向所有待发节点的发送所需时间为："+requestsHasSent+"ms");
    }

    public static long getSendRequestTime() {
        return sendRequestTime;
    }

    public static void setSendRequestTime(long sendRequestTime) {
        TimeTest.sendRequestTime = sendRequestTime;
        System.out.println(Thread.currentThread().getName() + "：发送一次请求耗费的时间为："+sendRequestTime+"ms");
    }

    public static long calculate(long start, long end){
        return (end - start);
    }
}
