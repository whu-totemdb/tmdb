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

    /*public static void showCostTreeMap(){
        for (String requestID : costTreeMap.keySet()){
            System.out.println("同步请求"+requestID+"各部分耗时为：");
            costTreeMap.get(requestID).showCosts();
        }
    }*/


}
