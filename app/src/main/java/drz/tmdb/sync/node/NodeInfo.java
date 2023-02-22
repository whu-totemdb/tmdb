package drz.tmdb.sync.node;

import java.net.InetSocketAddress;

public class NodeInfo {
    public InetSocketAddress socketAddress;

    public long error;

    public NodeState state;

    public NodeInfo(InetSocketAddress socketAddress, NodeState state) {
        this.socketAddress = socketAddress;
        this.state = state;
    }

    public NodeInfo(InetSocketAddress socketAddress, long error, NodeState state) {
        this.socketAddress = socketAddress;
        this.error = error;
        this.state = state;
    }
}
