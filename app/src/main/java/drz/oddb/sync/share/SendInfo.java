package drz.oddb.sync.share;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import drz.oddb.sync.network.GossipRequest;

public class SendInfo {
    private int structureSize;

    private ConcurrentLinkedQueue<Object[]> targets;//目标节点的IP套接字地址

    private ConcurrentLinkedQueue<GossipRequest> requestToSend;//待发送的请求


    public SendInfo(int structureSize) {
        this.structureSize = structureSize;
        targets = new ConcurrentLinkedQueue<>();
        requestToSend = new ConcurrentLinkedQueue<>();
    }

    public ConcurrentLinkedQueue<Object[]> getTargets() {
        return targets;
    }

    public ConcurrentLinkedQueue<GossipRequest> getRequestToSend() {
        return requestToSend;
    }


    public boolean structureIsFull(){
        return targets.size() >= structureSize;
    }

    public boolean structureIsEmpty(){
        return requestToSend.isEmpty();
    }
}
