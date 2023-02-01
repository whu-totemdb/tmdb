package drz.tmdb.sync.network;

import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.share.RequestType;
import drz.tmdb.sync.vectorClock.VectorClock;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class GossipRequest implements Serializable {
    public int batch_id;//供统计使用，后可删除

    private RequestType requestType;

    private String requestID;

    private Long key;//数据主键

    /*还需要添加一个属性为value，表示数据更新后的值*/
    private Action action;

    private VectorClock vectorClock;

    private InetSocketAddress sourceIPAddress;

    private InetSocketAddress targetIPAddress;

    private long sendTime;

    private long receiveTime;



    public GossipRequest(RequestType requestType, InetSocketAddress sourceIPAddress) {
        this.requestType = requestType;
        this.sourceIPAddress = sourceIPAddress;

    }


    public GossipRequest(String requestID, Long key, VectorClock vectorClock, InetSocketAddress sourceIPAddress, InetSocketAddress targetIPAddress) {
        this.requestID = requestID;
        this.key = key;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;
        this.targetIPAddress = targetIPAddress;

    }

    public GossipRequest(RequestType requestType, String requestID, Long key, Action action, VectorClock vectorClock, InetSocketAddress sourceIPAddress) {
        this.requestType = requestType;
        this.requestID = requestID;
        this.key = key;
        this.action = action;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;


    }

    public GossipRequest(String requestID, Long key, Action action, VectorClock vectorClock, InetSocketAddress sourceIPAddress, long sendTime) {
        this.requestID = requestID;
        this.key = key;
        this.action = action;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;
        this.sendTime = sendTime;

    }

    public RequestType getRequestType() {
        return requestType;
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

    public long getSendTime() {
        return sendTime;
    }

    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }

    public long getReceiveTime() {
        return receiveTime;
    }

    public void setReceiveTime(long receiveTime) {
        this.receiveTime = receiveTime;
    }

    public long getTransportTimeMillis(){
        return (receiveTime - sendTime);
    }



}
