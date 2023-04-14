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

    private final static int maxClockEntry = Short.MAX_VALUE;

    private final static int entryNumLimit = 50;

    private final TreeMap<String,Long> vectorClock;

    private volatile long timestamp;//时间戳

    //private ClockEntryHeap heap;

    public VectorClock(){
        this(System.currentTimeMillis());
    }

    public VectorClock(long timestamp){
        this.vectorClock = new TreeMap<String,Long>();
        this.timestamp = timestamp;
        //heap = new ClockEntryHeap(maxClockEntry,entryNumLimit);
    }

    /*public VectorClock(List<ClockEntry> versions, long timestamp) {
        this.vectorClock = new TreeMap<>();
        this.timestamp = timestamp;
        for(ClockEntry clockEntry: versions) {
            this.vectorClock.put(clockEntry.getNodeID(), clockEntry.getVersion());
        }
    }*/

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

    public void put(String nodeID,long version){
        vectorClock.put(nodeID,version);
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
        //heap.put(nodeID);
        this.timestamp = currentTime;

        System.out.print("更新后的向量时钟：");
        System.out.println(this.showVectorClock());

        if (vectorClock.size() >= maxClockEntry){
            //调用剪枝算法
        }

    }


    public static Relation compare(VectorClock vc1,VectorClock vc2){
        long start = System.currentTimeMillis();

        boolean vcLowBigger = false;
        boolean vcHighBigger = false;

        VectorClock vcLow,vcHigh;
        boolean vc1Low;

        if (vc1.getVectorClock().size() <= vc2.getVectorClock().size()){
            vcLow = vc1;
            vcHigh = vc2;
            vc1Low = true;
        }
        else {
            vcLow = vc2;
            vcHigh = vc1;
            vc1Low = false;
        }

        int vcLowCount = 0;
        int vcHighCount = 0;

        for (String key : vcLow.getVectorClock().keySet()){
            Long versionLow = vcLow.getVectorClock().get(key);
            Long versionHigh = vcHigh.getVectorClock().get(key);

            if (versionHigh == null){
                //高维向量对应维度不存在，说明低维向量在此维度上更大
                vcLowBigger = true;
            }
            else {
                if (versionLow > versionHigh){
                    vcLowBigger = true;
                }
                else if(versionLow < versionHigh){
                    vcHighBigger = true;
                }
                //若相同则不改变标志位

                vcHighCount++;
            }
            vcLowCount++;

            if (vcLowCount > 1 && vcLowBigger && vcHighBigger){
                //已经比较过至少两个维度且其中存在至少两个维度上，其中一个维度更大，另一个维度更小
                long end = System.currentTimeMillis();
                System.out.println("一次向量时钟比较耗时为："+(end-start)+"ms");
                return Relation.Parallel;
            }
        }

        //低维向量为0维
        if (vcLowCount == 0){
            if (vcHigh.getVectorClock().size() == 0){
                return Relation.Equal;
            }

            if (vc1Low){
                return Relation.Before;
            }
            else {
                return Relation.After;
            }
        }

        //低维向量只有一维
        if (vcLowCount == 1){
            //高维向量的维度仍未比较完，在这些维度上都是高维向量更大
            if (vcHighCount < vcHigh.getVectorClock().size()){
                long end = System.currentTimeMillis();
                System.out.println("一次向量时钟比较耗时为："+(end-start)+"ms");
                if (vcLowBigger){
                    //存在低维向量有某一维度更大的情况
                    return Relation.Parallel;
                }
                else {
                    //否则就是高维向量更大
                    if (vc1Low) {
                        //vc1是低维向量
                        return Relation.Before;
                    }
                    else {
                        return Relation.After;
                    }
                }
            }
            else {
                //高维向量的维度也全部比较完，那么低维和高维向量维度均为1且该维度都是对同一节点的观测
                long end = System.currentTimeMillis();
                System.out.println("一次向量时钟比较耗时为："+(end-start)+"ms");
                if (vcLowBigger){
                    if (vc1Low) {
                        //vc1是低维向量
                        return Relation.After;
                    }
                    else {
                        return Relation.Before;
                    }
                }
                else {
                    if (!vcHighBigger){
                        return Relation.Equal;
                    }

                    if (vc1Low) {
                        //vc1是低维向量
                        return Relation.Before;
                    }
                    else {
                        return Relation.After;
                    }
                }
            }

        }
        else {
            //低维向量维度超过一维
            //在循环体中的逻辑一定保证vcLowCount>=vcHighCount，且vcLowBigger和vcHighBigger不同时为true


            if (vcHighCount < vcHigh.getVectorClock().size()){
                //高维向量还存在维度尚未比较，在这些维度上高维向量更大

                long end = System.currentTimeMillis();
                System.out.println("一次向量时钟比较耗时为："+(end-start)+"ms");

                if (vcLowBigger) {
                    //在之前的比较中vcLow存在更大的维度
                    return Relation.Parallel;
                }
                else {
                    //vcLowBigger为false表示之前比较的维度中都是vcHigh更大，因此vcLow一定小于vcHigh，vcLow Before vcHigh
                    if (vc1Low) {
                        return Relation.Before;
                    }
                    else {
                        return Relation.After;
                    }
                }
            }
            else {
                //高维向量的维度也全部比较完成
                long end = System.currentTimeMillis();
                System.out.println("一次向量时钟比较耗时为："+(end-start)+"ms");
                if (vcHighBigger) {
                    if (vc1Low){
                        return Relation.Before;
                    }
                    else {
                        return Relation.After;
                    }
                }
                else {
                    if (!vcLowBigger){
                        return Relation.Equal;
                    }

                    if (vc1Low){
                        return Relation.After;
                    }
                    else {
                        return Relation.Before;
                    }
                }
            }

        }
    }


    /*public static Relation compare(VectorClock vc1, VectorClock vc2){
        long start = System.currentTimeMillis();
        //某一维度更大的标志位
        boolean vc1Bigger = false;
        boolean vc2Bigger = false;

        Set<String> keySet1 = vc1.getVectorClock().keySet();
        Set<String> keySet2 = vc2.getVectorClock().keySet();

        *//*boolean vc1Low = true;

        if (keySet1.size() > keySet2.size()){
            vc1Low = false;
        }
        Set<String> keySet = vc1Low ? keySet1 : keySet2;*//*

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
    }*/

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
                case Equal:
                    //保持原样
                    break;
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

    /*public List<ClockEntry> getClockEntries(){
        List<ClockEntry> clockEntries = new ArrayList<ClockEntry>();

        for (Map.Entry<String,Long> entry : vectorClock.entrySet()){
            clockEntries.add(new ClockEntry(entry.getKey(),entry.getValue()));
        }

        return clockEntries;
    }*/


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
