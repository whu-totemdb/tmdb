package drz.tmdb.sync.vectorClock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class VectorClock implements Serializable {

    private final static long ride = 1;

    private final int maxClockEntry = Short.MAX_VALUE;
    private final TreeMap<String,Long> vectorClock;

    private volatile long timestamp;//时间戳

    public VectorClock(){
        this(System.currentTimeMillis());
    }

    public VectorClock(long timestamp){
        this.vectorClock = new TreeMap<String,Long>();
        this.timestamp = timestamp;
    }

    public VectorClock(List<ClockEntry> versions, long timestamp) {
        this.vectorClock = new TreeMap<>();
        this.timestamp = timestamp;
        for(ClockEntry clockEntry: versions) {
            this.vectorClock.put(clockEntry.getNodeID(), clockEntry.getVersion());
        }
    }

    public VectorClock(TreeMap<String, Long> vectorClock, long timestamp) {
        if(vectorClock == null){
            throw new IllegalArgumentException("向量时钟为空");
        }

        this.vectorClock = vectorClock;
        this.timestamp = timestamp;
    }




    public TreeMap<String, Long> getVectorClock() {
        return vectorClock;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void increaseVersion(String nodeID,long currentTime){

        System.out.print("原来的向量时钟：");
        System.out.println(this.showVectorClock());

        Long version = vectorClock.get(nodeID);
        if (version == null){
            version = 1L;
        }
        else{
            version = version + ride;
        }

        vectorClock.put(nodeID,version);
        this.timestamp = currentTime;

        System.out.print("更新后的向量时钟：");
        System.out.println(this.showVectorClock());

        if (vectorClock.size() >= maxClockEntry){
            //调用剪枝算法
        }

    }


    //向量时钟两两比较算法
    public VectorClock merge(VectorClock clock){
        VectorClock newClock = new VectorClock();

        System.out.print("原来的向量时钟：");
        System.out.println(this.showVectorClock());

        for (Map.Entry<String,Long> entry : this.vectorClock.entrySet()){
            newClock.getVectorClock().put(entry.getKey(),entry.getValue());
        }

        for (Map.Entry<String,Long> entry : clock.vectorClock.entrySet()){
            Long version = newClock.getVectorClock().get(entry.getKey());


            if (version == null){
                //在原来的向量时钟中不存在
                newClock.getVectorClock().put(entry.getKey(),entry.getValue());
            }
            else {
                //已存在，则更新为二者之间的较大者
                if (entry.getValue() > version){
                    newClock.getVectorClock().put(entry.getKey(),entry.getValue());
                }
            }
        }

        System.out.print("合并后的向量时钟：");
        System.out.println(newClock.showVectorClock());
        return newClock;
    }

    public List<ClockEntry> getClockEntries(){
        List<ClockEntry> clockEntries = new ArrayList<ClockEntry>();

        for (Map.Entry<String,Long> entry : vectorClock.entrySet()){
            clockEntries.add(new ClockEntry(entry.getKey(),entry.getValue()));
        }

        return clockEntries;
    }


    public String showVectorClock(){
        /*String result = "[";

        for (Map.Entry<String,Long> entry : vectorClock.entrySet()){
            result = result + entry.getKey() + ":" + entry.getValue() + ", ";
        }

        result += "]";*/
        StringBuilder result =new StringBuilder();
        result.append("( ");
        result.append("[");
        int entryNumber = vectorClock.size();

        for (Map.Entry<String,Long> entry : vectorClock.entrySet()){
            entryNumber--;
            String nodeID = entry.getKey();
            Long version = entry.getValue();
            result.append(nodeID + "-" + version);
            if (entryNumber > 0){
                result.append(", ");
            }
        }

        result.append("],");
        result.append(" ts: " + timestamp + " )");

        return result.toString();
    }

}
