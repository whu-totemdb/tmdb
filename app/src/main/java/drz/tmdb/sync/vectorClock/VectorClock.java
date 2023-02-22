package drz.tmdb.sync.vectorClock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import drz.tmdb.sync.network.Response;
import drz.tmdb.sync.node.database.Action;

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

    public static Relation compare(VectorClock vc1, VectorClock vc2){
        long start = System.currentTimeMillis();
        //某一维度更大的标志位
        boolean vc1Bigger = false;
        boolean vc2Bigger = false;

        Set<String> keySet1 = vc1.getVectorClock().keySet();
        Set<String> keySet2 = vc2.getVectorClock().keySet();


        int vc1Count = 0;
        int vc2Count = 0;

        for (String key : keySet1){
            Long version1 = vc1.getVectorClock().get(key);
            Long version2 = vc2.getVectorClock().get(key);

            if (version2 == null){
                //说明version2为0，此维度上version1更大
                vc1Bigger = true;
            }
            else {
                if (version1 > version2){
                    vc1Bigger = true;
                }
                else if(version1 < version2){
                    vc2Bigger = true;
                }
                vc2Count++;
            }

            vc1Count++;

            if (vc1Count > 1 && vc1Bigger && vc2Bigger){
                //说明存在两个不同的维度，一个维度vc1>vc2，另一个维度vc1<vc2
                long end = System.currentTimeMillis();
                System.out.println("一次向量时钟比较耗时为："+(end-start)+"ms");
                return Relation.Parallel;
            }
        }

        if (vc1Count == 1){
            if (vc2Count < keySet2.size()){
                long end = System.currentTimeMillis();
                System.out.println("一次向量时钟比较耗时为："+(end-start)+"ms");
                if (vc1Bigger){
                    return Relation.Parallel;
                }
                else {
                    return Relation.Before;
                }
            }
            else {
                //vc1与vc2仅含有一个且相同的nodeID
                long end = System.currentTimeMillis();
                System.out.println("一次向量时钟比较耗时为："+(end-start)+"ms");
                if (vc1Bigger){
                    return Relation.After;
                }
                else {
                    return Relation.Before;
                }
            }
        }
        else {
            //由于上面循环中的逻辑，vc1Count >= vc2Count，且vc1Bigger和vc2Bigger不会同时为true
            if (vc2Count < keySet2.size()) {
                long end = System.currentTimeMillis();
                System.out.println("一次向量时钟比较耗时为："+(end-start)+"ms");
                //keySet2还有部分元素是keySet1没有的，说明在这些维度上vc2>vc1，而在之前的某些维度上vc1>vc2
                if (vc1Bigger) {
                    //在之前的比较中一直 都是vc1更大
                    return Relation.Parallel;
                } else {
                    //vc2Bigger为true表示之前比较的维度中都是vc2更大，为false表示之前的比较的各维度均相同，因此vc1一定小于vc2，vc1 Before vc2
                    return Relation.Before;
                }

            } else {
                long end = System.currentTimeMillis();
                System.out.println("一次向量时钟比较耗时为："+(end-start)+"ms");
                if (vc2Bigger) {
                    return Relation.Parallel;
                } else {
                    return Relation.After;
                }
            }
        }
    }

    public static Action getLatestVersion(ArrayList<Response> responses){
        Action result;
        Action action1,action2;
        VectorClock vc1,vc2;

        action1 = responses.get(0).getAction();
        vc1 = responses.get(0).getVectorClock();

        Response response;
        for (int i = 1; i < responses.size(); i++){
            response = responses.get(i);

            action2 = response.getAction();
            vc2 = response.getVectorClock();

            switch (compare(vc1,vc2)){
                case Before:
                    vc1 = vc2;
                    action1 = action2;
                    break;
                case After:
                    //保持原来的vc1和action1
                    break;
                case Parallel:
                    if (vc1.getTimestamp() < vc2.getTimestamp()){
                        vc1 = vc1.merge(vc2);//合并
                        action1 = action2;
                    }
                    else {
                        vc1 = vc2.merge(vc1);
                    }
                    break;
            }

        }

        result = action1;
        return result;
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
