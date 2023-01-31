package drz.oddb.sync.arbitration;

import java.util.ArrayList;
import java.util.HashMap;

import drz.oddb.sync.network.GossipRequest;

public class ArbitrationController {

    //请求号与对应的仲裁器的映射关系，只有协助节点才会生成仲裁器并存储
    private HashMap<String,Arbitration> arbitrationHashMap = new HashMap<>();

    //请求号与它收到的其他节点发回请求的映射关系，只有协助节点才需要存储
    private HashMap<String, ArrayList<GossipRequest>> requestMap = new HashMap<>();


}
