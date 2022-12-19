package drz.oddb.sync.network;

import drz.oddb.sync.node.database.Action;
import drz.oddb.sync.vectorClock.VectorClock;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;

public class GossipRequest implements Serializable {
    private Long key;//数据主键

    /*还需要添加一个属性为value，表示数据更新后的值*/
    private Action action;

    private VectorClock vectorClock;

    private InetAddress sourceIPAddress;

    private InetSocketAddress targetIPAddress;

    private long sendTime;

    private long receiveTime;

    public GossipRequest(Long key, VectorClock vectorClock) {
        this.key = key;
        this.vectorClock = vectorClock;
    }

    public GossipRequest(Long key, VectorClock vectorClock, InetAddress sourceIPAddress) {
        this.key = key;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;

    }


    public GossipRequest(Long key, VectorClock vectorClock, InetAddress sourceIPAddress, InetSocketAddress targetIPAddress) {
        this.key = key;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;
        this.targetIPAddress = targetIPAddress;

    }

    public GossipRequest(Long key, Action action, VectorClock vectorClock, InetAddress sourceIPAddress, long sendTime) {
        this.key = key;
        this.action = action;
        this.vectorClock = vectorClock;
        this.sourceIPAddress = sourceIPAddress;
        this.sendTime = sendTime;
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

    public InetAddress getSourceIPAddress() {
        return sourceIPAddress;
    }

    public void setSourceIPAddress(InetAddress sourceIPAddress) {
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
