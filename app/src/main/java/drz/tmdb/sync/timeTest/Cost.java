package drz.tmdb.sync.timeTest;

import java.io.Serializable;
import java.util.TreeMap;

public class Cost implements Serializable {
    private TreeMap<String,Long> costs = new TreeMap<>();

    public TreeMap<String, Long> getCosts() {
        return costs;
    }

    public void putCost(String name, long value){
        costs.put(name,value);
    }

    public long getCost(String name) {
        if (costs.get(name)==null){
            return -1;//不存在
        }
        else {
            return costs.get(name);
        }
    }

    public void showCosts(){

        for (String name : costs.keySet()){
            System.out.println(name + ":" + costs.get(name) + "ms");
        }

    }
}
