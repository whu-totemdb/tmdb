package drz.tmdb.sync.network;

import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.share.RequestType;
import drz.tmdb.sync.timeTest.Cost;
import drz.tmdb.sync.vectorClock.VectorClock;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashMap;

public class GossipRequest implements Serializable {

    private RequestType requestType;

    private String nodeID;

    private String requestID;

    private Long key;//数据主键

    private Action action;

    private VectorClock vectorClock;

    private InetSocketAddress sourceIPAddress;

    private InetSocketAddress targetIPAddress;


    //统计使用

    public long sendTime;//请求的发送时刻

    public long receiveTime;//请求的接收时刻



    public GossipRequest(RequestType requestType, String nodeID, InetSocketAddress sourceIPAddress) {
        this.requestType = requestType;
        this.nodeID = nodeID;
        this.sourceIPAddress = sourceIPAddress;

    }


    public GossipRequest(String requestID, Long key, VectorClock vectorClock, InetSocketAddress sourceIPAddress, InetSocketAddress targetIPAddress) {
        this.requestID = requestID;
        this.key = key;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;
        this.targetIPAddress = targetIPAddress;

    }

    public GossipRequest(RequestType requestType, String nodeID, String requestID, Long key, Action action, VectorClock vectorClock, InetSocketAddress sourceIPAddress) {
        this.requestType = requestType;
        this.nodeID = nodeID;
        this.requestID = requestID;
        this.key = key;
        this.action = action;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;


    }


    public RequestType getRequestType() {
        return requestType;
    }

    public String getNodeID() {
        return nodeID;
    }

    public String getRequestID() {
        return requestID;
    }

    public void setRequestID(String requestID) {
        this.requestID = requestID;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public Long getKey() {
        return key;
    }

    public void setKey(Long key) {
        this.key = key;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public void setVectorClock(VectorClock vectorClock) {
        this.vectorClock = vectorClock;
    }

    public InetSocketAddress getSourceIPAddress() {
        return sourceIPAddress;
    }

    public void setSourceIPAddress(InetSocketAddress sourceIPAddress) {
        this.sourceIPAddress = sourceIPAddress;
    }

    public InetSocketAddress getTargetIPAddress() {
        return targetIPAddress;
    }

    public void setTargetIPAddress(InetSocketAddress targetIPAddress) {
        this.targetIPAddress = targetIPAddress;
    }


}
