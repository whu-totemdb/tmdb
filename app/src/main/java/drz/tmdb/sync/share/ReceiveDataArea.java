package drz.tmdb.sync.share;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

import drz.tmdb.sync.network.GossipRequest;
import drz.tmdb.sync.network.Response;

public class ReceiveDataArea implements Serializable {
    private int areaSize;

    private ConcurrentLinkedQueue<GossipRequest> receivedRequestQueue;//gossip请求接收队列

    private ConcurrentLinkedQueue<Response> receivedResponseQueue;//响应的接收队列

    public ReceiveDataArea(int areaSize) {
        this.areaSize = areaSize;
        receivedRequestQueue = new ConcurrentLinkedQueue<>();
        receivedResponseQueue = new ConcurrentLinkedQueue<>();
    }

    public ConcurrentLinkedQueue<GossipRequest> getReceivedRequestQueue() {
        return receivedRequestQueue;
    }

    public ConcurrentLinkedQueue<Response> getReceivedResponseQueue() {
        return receivedResponseQueue;
    }

    public boolean requestQueueFull(){
        return receivedRequestQueue.size() >= areaSize;
    }

    public boolean requestQueueEmpty(){
        return receivedRequestQueue.isEmpty();
    }

    public boolean responseQueueFull(){
        return receivedResponseQueue.size() >= areaSize;
    }

    public boolean responseQueueEmpty(){
        return receivedResponseQueue.isEmpty();
    }
}
