package drz.oddb.sync.network;

import drz.oddb.sync.node.database.Action;
import drz.oddb.sync.timeTest.ReceiveTimeTest;
import drz.oddb.sync.timeTest.SendTimeTest;
import drz.oddb.sync.vectorClock.VectorClock;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;

public class GossipRequest implements Serializable {
    public int batch_id;//供统计使用，后可删除

    private int requestID;

    private Long key;//数据主键

    /*还需要添加一个属性为value，表示数据更新后的值*/
    private Action action;

    private VectorClock vectorClock;

    private InetSocketAddress sourceIPAddress;

    private InetSocketAddress targetIPAddress;

    private boolean broadcast;//本请求是否为广播请求的标志位

    private long sendTime;

    private long receiveTime;



    public GossipRequest(InetSocketAddress sourceIPAddress, boolean broadcast) {

        this.sourceIPAddress = sourceIPAddress;
        this.broadcast = broadcast;
    }


    public GossipRequest(int requestID, Long key, VectorClock vectorClock, InetSocketAddress sourceIPAddress, InetSocketAddress targetIPAddress) {
        this.requestID = requestID;
        this.key = key;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;
        this.targetIPAddress = targetIPAddress;

    }

    public GossipRequest(int requestID, Long key, Action action, VectorClock vectorClock, InetSocketAddress sourceIPAddress, boolean broadcast) {
        this.requestID = requestID;
        this.key = key;
        this.action = action;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;
        this.broadcast = broadcast;

    }

    public GossipRequest(int requestID, Long key, Action action, VectorClock vectorClock, InetSocketAddress sourceIPAddress, long sendTime) {
        this.requestID = requestID;
        this.key = key;
        this.action = action;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;
        this.sendTime = sendTime;

    }

    public int getRequestID() {
        return requestID;
    }

    public void setRequestID(int requestID) {
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

    public boolean isBroadcast() {
        return broadcast;
    }

    public void setBroadcast(boolean broadcast) {
        this.broadcast = broadcast;
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
