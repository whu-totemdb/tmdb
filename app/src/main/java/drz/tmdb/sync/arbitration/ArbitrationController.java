package drz.tmdb.sync.arbitration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

import drz.tmdb.sync.network.GossipRequest;
import drz.tmdb.sync.network.Response;

public class ArbitrationController implements Serializable {

    //请求号与对应的仲裁器的映射关系，只有协助节点才会生成仲裁器并存储
    private HashMap<String, Arbitration> arbitrationHashMap = new HashMap<>();

    //请求号与它收到的其他节点发回请求的映射关系，只有协助节点才需要存储
    private HashMap<String, ArrayList<Response>> responseMap = new HashMap<>();

    //生成一个仲裁器
    public void putArbitration(String requestID, Arbitration arbitration){
        arbitrationHashMap.put(requestID,arbitration);
        responseMap.put(requestID,new ArrayList<Response>());
    }

    public Arbitration getArbitrationByID(String requestID){
        return arbitrationHashMap.get(requestID);
    }

    public void putReceivedResponse(String requestID,Response response){
        responseMap.get(requestID).add(response);
        arbitrationHashMap.get(requestID).increase();//响应计数器加一
    }

    public ArrayList<Response> getResponsesByID(String requestID){
        return responseMap.get(requestID);
    }

    public void deleteArbitration(String requestID){
        arbitrationHashMap.remove(requestID);
        responseMap.remove(requestID);
    }
}
