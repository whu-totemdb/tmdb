package drz.tmdb.sync.share;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import drz.tmdb.sync.network.GossipRequest;

public class SendInfo implements Serializable {
    private int structureSize;

    private ConcurrentLinkedQueue<Object[]> targets;//目标节点的IP套接字地址

    private ConcurrentLinkedQueue<GossipRequest> requestToSend;//待发送的请求

    private HashMap<String,Integer> indexMap;//请求与其所在发送窗口内的索引的映射关系


    public SendInfo(int structureSize) {
        this.structureSize = structureSize;
        targets = new ConcurrentLinkedQueue<>();
        requestToSend = new ConcurrentLinkedQueue<>();
        indexMap = new HashMap<>();
    }

    public ConcurrentLinkedQueue<Object[]> getTargets() {
        return targets;
    }

    public ConcurrentLinkedQueue<GossipRequest> getRequestToSend() {
        return requestToSend;
    }

    public HashMap<String, Integer> getIndexMap() {
        return indexMap;
    }

    public boolean structureIsFull(){
        return targets.size() >= structureSize;
    }

    public boolean structureIsEmpty(){
        return requestToSend.isEmpty();
    }
}
