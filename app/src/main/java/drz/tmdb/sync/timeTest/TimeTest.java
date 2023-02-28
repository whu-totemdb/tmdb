package drz.tmdb.sync.timeTest;

import java.util.TreeMap;

public class TimeTest {
    private int dataSize;

    private static TreeMap<String,Cost> costTreeMap = new TreeMap<>();//请求id与该请求各项耗费的映射关系

    public static Cost getCosts(String requestID){
        if(costTreeMap.get(requestID)==null){
            return null;
        }
        else {
            return costTreeMap.get(requestID);
        }
    }

    public static long getCost(String requestID,String name) {
        Cost cost = costTreeMap.get(requestID);
        if (cost == null){
            return -1;
        }

        return cost.getCost(name);
    }

    public static void putInCostTreeMap(String requestID) {
        TimeTest.costTreeMap.put(requestID,new Cost());
    }

    public static void putInCostTreeMap(String requestID,String name,long value){
        Cost c = TimeTest.costTreeMap.get(requestID);

        if (c == null){
            Cost t = new Cost();
            t.putCost(name,value);
            TimeTest.costTreeMap.put(requestID,t);
        }
        else {
            c.putCost(name,value);
        }
    }

    public static void addCosts(String requestID,Cost cost){
        if(costTreeMap.get(requestID)==null){
            costTreeMap.put(requestID,cost);
        }
        else {
            Cost cost1 = costTreeMap.get(requestID);
            for (String name : cost.getCosts().keySet()){
                cost1.putCost(name,cost.getCost(name));
            }
        }
    }

    public static void showCost(String requestID){
        costTreeMap.get(requestID).showCosts();
    }

    public static void showCostTreeMap(){
        for (String requestID : costTreeMap.keySet()){
            System.out.println("同步请求"+requestID+"各部分耗时为：");
            costTreeMap.get(requestID).showCosts();
        }
    }



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
