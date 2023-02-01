package drz.tmdb.sync.timeTest;

public class ReceiveTimeTest {
    //private int dataSize;
    //接收过程
    private long readObjectTime;

    private long mergeVectorClockTime;

    public ReceiveTimeTest(){}

    /*public ReceiveTimeTest(int dataSize) {
        this.dataSize = dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }*/

    public long getReadObjectTime() {
        return readObjectTime;
    }

    public void setReadObjectTime(long readObjectTime) {
        this.readObjectTime = readObjectTime;
        System.out.println(Thread.currentThread().getName() + "：反序列化对象需要花费的时间为："+readObjectTime+"ms");
    }

    public long getMergeVectorClockTime() {
        return mergeVectorClockTime;
    }

    public void setMergeVectorClockTime(long mergeVectorClockTime) {
        this.mergeVectorClockTime = mergeVectorClockTime;
        System.out.println(Thread.currentThread().getName() + "：合并一次向量时钟花费的时间为："+mergeVectorClockTime+"ms");
    }

    public static long calculate(long start, long end){
        return (end - start);
    }
}
