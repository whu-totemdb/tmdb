package drz.oddb.sync.share;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import drz.oddb.sync.network.GossipRequest;

public class SendInfo {
    private int structureSize;

    private ConcurrentLinkedQueue<Object[]> targets;//目标节点的IP套接字地址

    private ConcurrentLinkedQueue<GossipRequest> requestToSend;//待发送的请求

    private ConcurrentHashMap<Integer,Boolean> requestSendOver;//请求是否发送完成

    public SendInfo(int structureSize) {
        this.structureSize = structureSize;
        targets = new ConcurrentLinkedQueue<>();
        requestToSend = new ConcurrentLinkedQueue<>();
        requestSendOver = new ConcurrentHashMap<>(structureSize);
    }

    public ConcurrentLinkedQueue<Object[]> getTargets() {
        return targets;
    }

    public ConcurrentLinkedQueue<GossipRequest> getRequestToSend() {
        return requestToSend;
    }

    public ConcurrentHashMap<Integer, Boolean> getRequestSendOver() {
        return requestSendOver;
    }

    public boolean structureIsFull(){
        return requestSendOver.size() >= structureSize;
    }

    public boolean structureIsEmpty(){
        return targets.isEmpty();
    }
}
