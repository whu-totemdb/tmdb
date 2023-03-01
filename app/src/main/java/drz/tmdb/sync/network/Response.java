package drz.tmdb.sync.network;

import java.io.Serializable;
import java.net.InetSocketAddress;

import drz.tmdb.sync.node.database.Action;
import drz.tmdb.sync.share.ResponseType;
import drz.tmdb.sync.timeTest.Cost;
import drz.tmdb.sync.vectorClock.VectorClock;

public class Response implements Serializable {
    private ResponseType responseType;

    private String requestID;

    private String nodeID;

    private Action action;

    private VectorClock vectorClock;

    private InetSocketAddress source;

    private InetSocketAddress target;

    private long sendTime;//广播响应发送的时刻或读写响应的发送时刻

    private long receiveTime;//广播请求接收的时刻或读写响应的接收时刻

    //测试时间使用
    private Cost cost;

    public long requestProcessTime;


    public Response(){}

    public Response(String nodeID,String requestID, ResponseType responseType, InetSocketAddress source, InetSocketAddress target) {
        this.nodeID = nodeID;
        this.requestID = requestID;
        this.responseType = responseType;
        this.source = source;
        this.target = target;
    }

    public Response(String nodeID,String requestID, ResponseType responseType, Action action, VectorClock vectorClock, InetSocketAddress source, InetSocketAddress target) {
        this.nodeID = nodeID;
        this.requestID = requestID;
        this.responseType = responseType;
        this.action = action;
        this.vectorClock = vectorClock;
        this.source = source;
        this.target = target;
    }

    public ResponseType getResponseType() {
        return responseType;
    }

    public InetSocketAddress getSource() {
        return source;
    }

    public InetSocketAddress getTarget() {
        return target;
    }

    public String getRequestID() {
        return requestID;
    }

    public String getNodeID() {
        return nodeID;
    }

    public Action getAction() {
        return action;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
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

    public void setCost(Cost cost) {
        this.cost = cost;
    }

    public Cost getCost() {
        return cost;
    }
}
