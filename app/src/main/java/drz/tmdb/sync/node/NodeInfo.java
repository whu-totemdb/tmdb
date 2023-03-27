package drz.tmdb.sync.node;


import java.io.Serializable;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import drz.tmdb.sync.timer.MyTimer;


public class NodeInfo implements Serializable {
    public InetSocketAddress socketAddress;

    public long error;

    public NodeState state;

    public long lastUpdateTime;

    public MyTimer timer;


    public NodeInfo(InetSocketAddress socketAddress, NodeState state) {
        this.socketAddress = socketAddress;
        this.state = state;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public NodeInfo(InetSocketAddress socketAddress, long error, NodeState state) {
        this.socketAddress = socketAddress;
        this.error = error;
        this.state = state;
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public void startTimer(ConcurrentHashMap<String,NodeInfo> cluster,String nodeID){
        timer = new MyTimer(10000,30000,10);
        timer.start(new TimerTask() {
            @Override
            public void run() {
                if (state == NodeState.active) {
                    if ((System.currentTimeMillis() - lastUpdateTime) > 20000) {
                        state = NodeState.fail;
                    }
                    else {
                        timer.count = 0;
                    }
                }else {
                    if ((System.currentTimeMillis() - lastUpdateTime) <= 20000){
                        state = NodeState.active;
                    }
                    else {
                        if (timer.count <= timer.maxCount){
                            timer.count++;
                        }
                        else {
                            //移出节点状态表
                            cluster.remove(nodeID);
                            timer.stop();
                        }
                    }
                }
            }
        });
    }
}
